# Variables
CARGO := cargo
RUSTUP := rustup
CBINDGEN := $(shell command -v cbindgen 2> /dev/null)
IOS_TARGETS := aarch64-apple-ios x86_64-apple-ios
ANDROID_TARGETS := aarch64-linux-android armv7-linux-androideabi
MACOS_TARGETS := x86_64-apple-darwin aarch64-apple-darwin
LIB_NAME := libxaeroflux
FLUTTER_DIR := ../flutter_app
VERSION := $(shell grep version Cargo.toml | head -n 1 | cut -d '"' -f 2)

# Default target
all: build-all

# Install all required tools and dependencies
setup:
	$(RUSTUP) target add $(IOS_TARGETS) $(ANDROID_TARGETS) $(MACOS_TARGETS)
	$(CARGO) install cbindgen cargo-deny cargo-audit
	# Install Android NDK if needed
	@echo "Please ensure Android NDK is installed and configured"

# Build for all platforms
build-all: ios android macos

# Check for cbindgen before using it
check-cbindgen:
ifndef CBINDGEN
	$(error "cbindgen is not installed. Please run 'cargo install cbindgen' first")
endif

# iOS builds
ios: check-cbindgen $(IOS_TARGETS:%=ios-%)
	@mkdir -p dist/ios
	$(CBINDGEN) --config cbindgen.toml --output dist/ios/$(LIB_NAME).h
	@echo "iOS libraries built in dist/ios/"

ios-%:
	@echo "Building for iOS target $*..."
	$(CARGO) build --target $* --release
	@mkdir -p dist/ios/$*
	cp target/$*/release/$(LIB_NAME).a dist/ios/$*/

# Create iOS universal library
ios-universal: ios
	@mkdir -p dist/ios/universal
	lipo -create $(IOS_TARGETS:%=dist/ios/%/$(LIB_NAME).a) -output dist/ios/universal/$(LIB_NAME).a

# Android builds
android: check-cbindgen $(ANDROID_TARGETS:%=android-%)
	@mkdir -p dist/android
	$(CBINDGEN) --config cbindgen.toml --output dist/android/$(LIB_NAME).h
	@echo "Android libraries built in dist/android/"

android-%:
	@echo "Building for Android target $*..."
	$(CARGO) build --target $* --release
	@mkdir -p dist/android/$*
	cp target/$*/release/$(LIB_NAME).so dist/android/$*/

# macOS builds
macos: check-cbindgen $(MACOS_TARGETS:%=macos-%)
	@mkdir -p dist/macos
	$(CBINDGEN) --config cbindgen.toml --output dist/macos/$(LIB_NAME).h
	@echo "macOS libraries built in dist/macos/"

macos-%:
	@echo "Building for macOS target $*..."
	$(CARGO) build --target $* --release
	@mkdir -p dist/macos/$*
	cp target/$*/release/$(LIB_NAME).dylib dist/macos/$*/

# Create macOS universal library
macos-universal: macos
	@mkdir -p dist/macos/universal
	lipo -create $(MACOS_TARGETS:%=dist/macos/%/$(LIB_NAME).dylib) -output dist/macos/universal/$(LIB_NAME).dylib

# Package for Flutter
flutter-package: ios-universal android macos-universal
	@mkdir -p $(FLUTTER_DIR)/ios/libs $(FLUTTER_DIR)/android/app/src/main/jniLibs/arm64-v8a $(FLUTTER_DIR)/macos/libs
	cp dist/ios/universal/$(LIB_NAME).a $(FLUTTER_DIR)/ios/libs/
	cp dist/ios/$(LIB_NAME).h $(FLUTTER_DIR)/ios/libs/
	cp dist/android/aarch64-linux-android/$(LIB_NAME).so $(FLUTTER_DIR)/android/app/src/main/jniLibs/arm64-v8a/
	cp dist/android/armv7-linux-androideabi/$(LIB_NAME).so $(FLUTTER_DIR)/android/app/src/main/jniLibs/armeabi-v7a/
	cp dist/macos/universal/$(LIB_NAME).dylib $(FLUTTER_DIR)/macos/libs/
	cp dist/macos/$(LIB_NAME).h $(FLUTTER_DIR)/macos/libs/
	@echo "Libraries packaged for Flutter integration"

# Run the full test suite
test: lint unit-tests integration-tests ffi-tests

# Linting
lint:
	$(CARGO) clippy --all-targets --all-features -- -D warnings
	$(CARGO) fmt --all -- --check

# Unit tests
unit-tests:
	$(CARGO) test --lib

# Integration tests
integration-tests:
	$(CARGO) test --test '*'

# FFI tests (requires building test harnesses)
ffi-tests:
	cd ffi-tests && $(CARGO) run

# Security audit
security:
	$(CARGO) audit
	$(CARGO) deny check

# Clean everything
clean:
	$(CARGO) clean
	rm -rf dist

# Create a release
release:
	@echo "Creating release v$(VERSION)"
	git tag -a v$(VERSION) -m "Release v$(VERSION)"
	git push origin v$(VERSION)

# Local development helpers
dev:
	$(CARGO) build
	$(CARGO) doc --open

# Run benchmarks
bench:
	$(CARGO) bench

# Phony targets
.PHONY: all setup build-all check-cbindgen ios ios-universal android macos macos-universal \
        flutter-package test lint unit-tests integration-tests ffi-tests security \
        clean release dev bench $(IOS_TARGETS:%=ios-%) $(ANDROID_TARGETS:%=android-%) \
        $(MACOS_TARGETS:%=macos-%)