// audio-worklet.js — the web platform's real-time audio OUTPUT (an AudioWorklet processor).
// A continuous mono stream read from a ring buffer that the AUDIO unit fills: the host pulls
// mixer PCM at real time and transfers each chunk to this processor's port; `process()` reads +
// Catmull-Rom (4-point cubic) resamples from the core mixer rate (srcRate) to the device rate (the
// AudioWorkletGlobalScope `sampleRate`). This replaces the bring-up setInterval + per-chunk
// AudioBufferSourceNode scheduling, so playback is gapless (no crackle at chunk seams) and
// latency stays bounded. Plain JS that runs in AudioWorkletGlobalScope (no module graph, no
// types, no SharedArrayBuffer) — it lives in native/ alongside the env js-library as the
// platform's untyped runtime glue (this dir is outside the typed host + the lint graph).
/* global AudioWorkletProcessor, registerProcessor, sampleRate, currentFrame */

const CAP = 1 << 17; // ring capacity in mono samples (~2.97 s @ 44.1 kHz) — must exceed maxLag
const READPOST_FRAMES = 512; // post the consumed read cursor to the worker every N output frames (4.1)
const PRIME_FRAC = 0.5; // start playback once the ring holds >= this fraction of the cushion (4.2)
const SRC_RATE = 44_100; // the AUDIO unit's FIXED mixer output rate (J2ME_AUDIO_OUT_RATE) (4.4)
const FADE_LEN = 64; // de-click ramp length in OUTPUT samples (~1.3 ms @ 48 kHz)
const UNDERFLOW_DECAY = 0.99; // per-sample decay of the held value during a dropout (fade to 0)
const ONSET_THRESH = 0.01; // |output| above this counts as the first AUDIBLE sample after a mark

class PcmRingProcessor extends AudioWorkletProcessor {
  constructor() {
    super();
    this.ring = new Float32Array(CAP);
    this.writeIdx = 0; // total samples written (monotonic)
    this.readPos = 0; // fractional read cursor in total-sample units
    this.srcRate = SRC_RATE; // the core mixer rate (FIXED 44.1 kHz); a {type:"rate"} message confirms it
    this.rateKnown = false; // emit silence until the worker confirms the source rate (4.4) — never
    this.cushionS = 0.08; // target buffered latency (the worker's cushion); grown when hidden
    // De-click state: hold the last output value so a dropout fades to 0 (no hard click) and a
    // resume / latency-drop ramps back in from it (no discontinuity click).
    this.lastV = 0;
    this.resumeFrom = 0; // the value to ramp back FROM after a dropout / drop
    this.fadeRamp = 0; // remaining OUTPUT samples of the resume ramp
    // Objective output-pipeline stats (gated; OFF by default — zero overhead in normal runs).
    // underflow = output samples emitted as a fade because the ring was empty AFTER playback
    // began (a dropout); lag = ring fill ahead of the read cursor (the buffered latency).
    this.stats = false;
    this.started = false;
    // Priming (4.2): hold playback silent until the ring has buffered past PRIME_FRAC of the cushion,
    // so the stream STARTS from a healthy buffer (not 1 sample) and the first audible sample ramps
    // from zero — no immediate underrun, no startup click. A one-time gate (subsequent gaps use the
    // dropout/resume de-click), so a track change does not re-pay the priming latency.
    this.primed = false;
    this.underflow = 0;
    this.framesSincePost = 0;
    // Closed-loop pacing (4.1): the consumed read cursor is reported to the worker every
    // READPOST_FRAMES output frames so it tops the ring to a target fill from ACTUAL consumption
    // (not wall-clock elapsed*rate) — this removes producer/consumer clock drift, so the ring
    // neither grows (latency) nor starves (underrun) and the worklet never has to drop (no clicks).
    this.framesSinceReadPost = 0;
    // Post-worklet OUTPUT tap (gated; OFF by default): copies this processor's OWN output (ch0) to
    // a ring posted back to the worker, so the audio-quality harness can measure the PLATFORM's
    // resample/pacing artifacts (post-worklet, at the device rate) SEPARATELY from the core mixer's
    // (the pre-worklet capture). Self-bounded by a sample budget so it auto-disarms.
    this.tap = false;
    this.tapBuf = null;
    this.tapLen = 0;
    this.tapRemaining = 0;
    // Onset latency: when a clip-control is sent, the worker marks the current frame; the first
    // AUDIBLE output sample after the mark gives onsetMs in this processor's own AudioContext clock
    // (the host adds the device baseLatency + the buffered cushion). -1 = not watching.
    this.onsetMark = -1;
    this.port.onmessage = (ev) => {
      const d = ev.data;
      if (d && d.type === "rate") {
        this.srcRate = d.rate > 0 ? d.rate : this.srcRate;
        this.rateKnown = true; // the source rate is now confirmed — playback may proceed (4.4)
        return;
      }
      if (d && d.type === "cushion") {
        // The worker's target cushion changed (e.g. grown while the tab is hidden so a throttled
        // pump timer doesn't starve the ring). Bound the lag to it so we don't constantly drop.
        this.cushionS = d.seconds > 0 ? d.seconds : this.cushionS;
        return;
      }
      if (d && d.type === "stats") {
        // Toggle the gated stats + reset the measurement counters ONLY — never reset `started`
        // (4.2): doing so mid-playback would drop the worklet back to its pre-playback branch and
        // glitch the output / mis-attribute underflow. `started` reflects real playback, not stats.
        this.stats = !!d.on;
        this.underflow = 0;
        this.framesSincePost = 0;
        return;
      }
      if (d && d.type === "tap") {
        // Arm/disarm the post-worklet output tap for d.ms of OUTPUT (at the device sampleRate).
        this.tap = d.ms > 0;
        this.tapRemaining = this.tap ? Math.round((d.ms / 1000) * sampleRate) : 0;
        this.tapBuf = this.tap ? new Float32Array(4096) : null;
        this.tapLen = 0;
        return;
      }
      if (d && d.type === "onsetMark") {
        // Stamp the current output frame; the next audible output sample yields onsetMs.
        this.onsetMark = currentFrame;
        return;
      }
      // Otherwise a Float32Array mono PCM chunk to enqueue (silence included, to keep the
      // stream continuous and the read cursor in lockstep with the producer).
      const chunk = d;
      // Validate the inbound chunk (4.2): ignore anything that is not a Float32Array (a stray
      // control message or a structured-clone mishap) rather than indexing it as PCM. Non-finite
      // SAMPLES are sanitized to 0 in the enqueue loop below — one NaN/Inf must never poison the
      // ring (it would propagate through the interpolation as a click / silence).
      if (!(chunk instanceof Float32Array)) {
        return;
      }
      // Never overwrite UNREAD samples: if the chunk does not fit in the ring's free space, drop
      // the oldest (de-clicked) to make room FIRST, so a hidden-tab burst (a chunk larger than the
      // ring, or piled past the read cursor) can never wrap over data the reader has not consumed.
      if (chunk.length > CAP - 2 - (this.writeIdx - this.readPos)) {
        this.readPos = this.writeIdx + chunk.length - (CAP - 2);
        this.resumeFrom = this.lastV;
        this.fadeRamp = FADE_LEN;
      }
      for (let i = 0; i < chunk.length; i++) {
        const s = chunk[i];
        this.ring[this.writeIdx % CAP] = Number.isFinite(s) ? s : 0; // sanitize NaN/Inf (4.2)
        this.writeIdx++;
      }
      // Bound output latency: if the buffer ran far ahead of playback, skip the read cursor forward
      // (drop the oldest) so latency cannot grow unbounded. The bound tracks the target cushion but
      // is CAPPED at the ring (CAP-2) so the buffered data always fits; the drop is de-clicked by
      // ramping the next output samples in from the last value.
      const maxLag = Math.min((this.cushionS + 0.15) * this.srcRate, CAP - 2);
      if (this.writeIdx - this.readPos > maxLag) {
        this.readPos = this.writeIdx - this.cushionS * this.srcRate;
        this.resumeFrom = this.lastV;
        this.fadeRamp = FADE_LEN;
      }
    };
  }

  process(_inputs, outputs) {
    const out = outputs[0];
    if (!out || out.length === 0) {
      return true;
    }
    const ch0 = out[0];
    const frames = ch0.length;
    // Source-rate gate (4.4): until the worker confirms the mixer rate, do NOT resample with a
    // guessed rate — emit silence. (srcRate already defaults to the fixed 44.1 kHz mixer rate, but
    // gating on the explicit signal means a wrong/late rate can never play at the wrong pitch.)
    if (!this.rateKnown) {
      for (let i = 0; i < frames; i++) {
        ch0[i] = 0;
      }
      for (let c = 1; c < out.length; c++) {
        out[c].set(ch0);
      }
      return true;
    }
    // Priming gate (4.2): until the ring has buffered past PRIME_FRAC of the cushion, emit silence
    // and hold the cursor — so playback STARTS from a healthy buffer (not the first sample). On the
    // prime transition, arm a ramp-from-zero (resumeFrom=0) so the first audible sample fades in.
    if (!this.primed) {
      if (this.writeIdx - this.readPos >= PRIME_FRAC * this.cushionS * this.srcRate) {
        this.primed = true;
        this.resumeFrom = 0;
        this.fadeRamp = FADE_LEN;
      } else {
        for (let i = 0; i < frames; i++) {
          ch0[i] = 0;
        }
        for (let c = 1; c < out.length; c++) {
          out[c].set(ch0);
        }
        return true; // still priming: silence, cursor held
      }
    }
    const step = this.srcRate / sampleRate;
    for (let i = 0; i < frames; i++) {
      let v;
      if (this.readPos + 1 < this.writeIdx) {
        // 4-point Catmull-Rom (cubic) resample (4.3): interpolate between s0 and s1 using the
        // neighbours sm1, s2 — a far smoother curve than the straight chord of linear interpolation,
        // so non-integer rate ratios (mixer 44.1 kHz -> any device rate) add much less imaging/THD.
        // Clamp the look-back at the very first sample and the look-ahead at the buffer edge.
        const i0 = Math.floor(this.readPos);
        const t = this.readPos - i0;
        const s0 = this.ring[i0 % CAP];
        const s1 = this.ring[(i0 + 1) % CAP];
        const sm1 = i0 >= 1 ? this.ring[(i0 - 1) % CAP] : s0;
        const s2 = i0 + 2 < this.writeIdx ? this.ring[(i0 + 2) % CAP] : s1;
        let real =
          0.5 *
          (2 * s0 +
            (-sm1 + s1) * t +
            (2 * sm1 - 5 * s0 + 4 * s1 - s2) * t * t +
            (-sm1 + 3 * s0 - 3 * s1 + s2) * t * t * t);
        this.readPos += step;
        this.started = true;
        if (this.fadeRamp > 0) {
          // De-click a resume (after a dropout or a latency drop): ramp from the held value to
          // the real sample so there is no step discontinuity (a click).
          const a = this.fadeRamp / FADE_LEN; // 1 -> 0
          real = real * (1 - a) + this.resumeFrom * a;
          this.fadeRamp--;
        }
        v = real;
      } else if (this.started) {
        // Dropout: de-click by decaying the held value toward 0 instead of a hard jump, and arm
        // the resume ramp so playback fades back in cleanly when the ring refills.
        this.lastV *= UNDERFLOW_DECAY;
        v = this.lastV;
        this.resumeFrom = this.lastV;
        this.fadeRamp = FADE_LEN;
        if (this.stats) {
          this.underflow++;
        }
      } else {
        v = 0; // pre-playback silence (the ring has not filled yet)
      }
      ch0[i] = v;
      this.lastV = v;
    }
    // Mirror mono to any extra channels (stereo device output).
    for (let c = 1; c < out.length; c++) {
      out[c].set(ch0);
    }
    // Closed-loop pacing (4.1): report the consumed read cursor (total source samples played) so
    // the worker tops the ring from ACTUAL consumption. Always on (cheap) — it IS the pacing loop.
    this.framesSinceReadPost += frames;
    if (this.framesSinceReadPost >= READPOST_FRAMES) {
      this.framesSinceReadPost = 0;
      this.port.postMessage({ type: "readpos", readPos: this.readPos });
    }
    // Onset latency: once marked, find the first AUDIBLE output sample and report the elapsed
    // output time since the mark (this processor's own AudioContext clock; sample-accurate).
    if (this.onsetMark >= 0) {
      for (let i = 0; i < frames; i++) {
        if (ch0[i] > ONSET_THRESH || ch0[i] < -ONSET_THRESH) {
          this.port.postMessage({
            onsetMs: ((currentFrame + i - this.onsetMark) / sampleRate) * 1000,
            type: "onset",
          });
          this.onsetMark = -1;
          break;
        }
      }
    }
    // Post-worklet tap: copy this quantum's OUTPUT (ch0) into the capture ring; transfer a full
    // chunk to the worker, and a final partial + "postdone" when the sample budget is spent.
    if (this.tap && this.tapBuf) {
      let n = frames;
      if (n > this.tapRemaining) {
        n = this.tapRemaining;
      }
      for (let i = 0; i < n; i++) {
        this.tapBuf[this.tapLen++] = ch0[i];
        if (this.tapLen === this.tapBuf.length) {
          this.port.postMessage({ pcm: this.tapBuf, rate: sampleRate, type: "postpcm" }, [
            this.tapBuf.buffer,
          ]);
          this.tapBuf = new Float32Array(4096);
          this.tapLen = 0;
        }
      }
      this.tapRemaining -= n;
      if (this.tapRemaining <= 0) {
        this.port.postMessage({ pcm: this.tapBuf.slice(0, this.tapLen), rate: sampleRate, type: "postpcm" });
        this.port.postMessage({ rate: sampleRate, type: "postdone" });
        this.tap = false;
        this.tapBuf = null;
        this.tapLen = 0;
      }
    }
    // Gated objective stats: report cumulative underflow + the instantaneous buffered lag
    // (latency = lag/srcRate seconds) to the worker ~2x/s, for the audio-quality harness.
    if (this.stats) {
      this.framesSincePost += frames;
      if (this.framesSincePost >= sampleRate / 2) {
        this.framesSincePost = 0;
        const lag = this.writeIdx - this.readPos;
        this.port.postMessage({
          type: "stats",
          underflow: this.underflow,
          lagSamples: lag > 0 ? lag : 0,
          srcRate: this.srcRate,
          deviceRate: sampleRate,
        });
      }
    }
    return true; // keep the processor alive
  }
}

registerProcessor("pcm-ring", PcmRingProcessor);
