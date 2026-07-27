#!/usr/bin/env bash
# Builds Liters.xcframework + Swift bindings for iOS.
#
# Prereqs:
#   rustup target add aarch64-apple-ios aarch64-apple-ios-sim x86_64-apple-ios
#   Xcode command line tools
#
# SQLITE= selects how SQLite is linked:
#
#   bundled          (default) compile the amalgamation into the staticlib.
#                    Self-contained; correct only if NOTHING ELSE in the host
#                    process links its own SQLite.
#   system           link the platform's /usr/lib/libsqlite3.dylib instead.
#                    REQUIRED when the app already links a SQLite — e.g. any
#                    app using GRDB, which links Apple's system libsqlite3 via
#                    `.systemLibrary(name: "CSQLite")`. Two SQLite copies in
#                    one process do not share the process-global inode table
#                    that works around POSIX's "close any fd, lose all locks"
#                    rule, so one can silently drop the other's advisory
#                    locks — and liters' correctness rests on a long-running
#                    read lock.
#   system-bindgen   as `system`, but regenerate the bindings from the
#                    platform's own sqlite3.h (needs libclang).
set -euo pipefail
cd "$(dirname "$0")/.."

SQLITE=${SQLITE:-bundled}
case "$SQLITE" in
  bundled)        FEATURE_ARGS=() ;;
  system)         FEATURE_ARGS=(--no-default-features) ;;
  system-bindgen) FEATURE_ARGS=(--no-default-features --features system-sqlite-bindgen) ;;
  *) echo "unknown SQLITE=$SQLITE (want: bundled | system | system-bindgen)" >&2; exit 2 ;;
esac
echo "sqlite linkage: $SQLITE"

OUT=target/apple
BINDINGS=$OUT/swift
DEVICE_TARGET=aarch64-apple-ios
SIM_TARGETS=(aarch64-apple-ios-sim x86_64-apple-ios)

for t in "$DEVICE_TARGET" "${SIM_TARGETS[@]}"; do
  cargo build -p liters-ffi --release --target "$t" "${FEATURE_ARGS[@]}"
done

# Generate Swift bindings from the host library's embedded metadata. Built with
# the SAME features as the device libraries: a feature that changes the exported
# API would otherwise produce bindings that do not match the shipped staticlib.
cargo build -p liters-ffi --release "${FEATURE_ARGS[@]}"
rm -rf "$BINDINGS" && mkdir -p "$BINDINGS"
cargo run -p liters-ffi --bin uniffi-bindgen -- generate \
  --library target/release/libliters_ffi.dylib \
  --language swift --out-dir "$BINDINGS"

# Headers directory for the xcframework: the C header + module map.
HEADERS=$OUT/headers
rm -rf "$HEADERS" && mkdir -p "$HEADERS"
cp "$BINDINGS"/*.h "$HEADERS"/
# uniffi emits a .modulemap; xcodebuild wants module.modulemap
cp "$BINDINGS"/*.modulemap "$HEADERS"/module.modulemap

# Fat simulator library.
mkdir -p "$OUT/sim"
lipo -create \
  $(for t in "${SIM_TARGETS[@]}"; do echo "target/$t/release/libliters_ffi.a"; done) \
  -output "$OUT/sim/libliters_ffi.a"

rm -rf "$OUT/Liters.xcframework"
xcodebuild -create-xcframework \
  -library "target/$DEVICE_TARGET/release/libliters_ffi.a" -headers "$HEADERS" \
  -library "$OUT/sim/libliters_ffi.a" -headers "$HEADERS" \
  -output "$OUT/Liters.xcframework"

echo "xcframework: $OUT/Liters.xcframework"
echo "swift sources: $BINDINGS/*.swift (add to your SPM target alongside the xcframework)"
