# Web soundfont asset — provenance

`soundfont.sf2` (served here at `/audio/soundfont.sf2`) is a **build output**: it
is copied from the vendored source by `platforms/web/scripts/build-wasm.sh` and is
NOT committed (see `.gitignore`). Only this note is checked in.

The shell fetches it **lazily**, on demand after the handshake (NOT in the initial
page bundle — the shell delivers it via the protocol's `SoundfontData` message
over the shell<->synth channel to the dedicated synth worker, which reports
`AudioSoundfontState`). A missing asset just means MIDI plays silence + a status
report, never a crash.

## Default bank

By default the copied bank is the **OpenJDK / Gervill General MIDI soundbank**
(`soundbank-emg.sf2`, 1 878 120 bytes) — the same GM default the desktop host
uses. See `vendor/fluidsynth/sf2/SOUNDBANK-PROVENANCE.md` for its full provenance,
SHA-256, the owner's licence ruling, and the master-gain calibration for this bank
on the float render path. Override the staged source with
`ROBUSTA_WEB_SOUNDFONT=<path>` when running `build-wasm.sh`.

FluidSynth itself needs no built-in soundfont — the soundbank is always a runtime
asset (see `vendor/fluidsynth/README-upstream.md`).
