#!/usr/bin/env bash
# install_tmux.sh — Install/reinstall tmux for Git Bash (MSYS2) on Windows 11
# Run as Administrator in Git Bash:  bash install_tmux.sh
# If not admin, installs to ~/bin and updates PATH.
set -euo pipefail

TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

MSYS2_REPO="https://repo.msys2.org/msys/x86_64"

# Decide install target: /usr/bin if writable, else ~/bin
if [ -w /usr/bin ]; then
    INSTALL_DIR="/usr/bin"
    LIB_DIR="/usr/bin"
    echo "==> Installing to /usr/bin (admin mode)"
else
    INSTALL_DIR="$HOME/bin"
    LIB_DIR="$HOME/bin"
    mkdir -p "$INSTALL_DIR"
    echo "==> No write access to /usr/bin — installing to ~/bin"
    echo "    (add ~/bin to PATH in your .bashrc if not already there)"
fi

echo "==> Downloading packages..."

# 1. Bootstrap zstd from GitHub (Windows native binary)
echo "    Downloading zstd bootstrap..."
curl -fSL -o "$TMP_DIR/zstd.zip" \
    https://github.com/facebook/zstd/releases/download/v1.5.7/zstd-v1.5.7-win64.zip
unzip -qo "$TMP_DIR/zstd.zip" -d "$TMP_DIR/zstd"
ZSTD="$TMP_DIR/zstd/zstd-v1.5.7-win64/zstd.exe"
echo "    bootstrapped: $("$ZSTD" --version)"

# Helper: decompress .tar.zst → .tar
decompress_zst() {
    local zst_file="$1"
    local tar_file="${zst_file%.zst}"
    "$ZSTD" -d "$(cygpath -w "$zst_file")" -o "$(cygpath -w "$tar_file")" --force -q
    echo "$tar_file"
}

# 2. libevent (tmux dependency)
echo "    Downloading libevent..."
curl -fSL -o "$TMP_DIR/libevent.tar.zst" \
    "$MSYS2_REPO/libevent-2.1.12-4-x86_64.pkg.tar.zst"
libevent_tar=$(decompress_zst "$TMP_DIR/libevent.tar.zst")
mkdir -p "$TMP_DIR/libevent"
cd "$TMP_DIR/libevent" && tar -xf "$libevent_tar" usr/bin/
cp -f "$TMP_DIR/libevent/usr/bin/"*.dll "$LIB_DIR/" 2>/dev/null || true
echo "    libevent DLLs installed"

# 3. tmux — try 3.4 first (best compat with Git Bash MSYS2 runtime)
echo "    Downloading tmux 3.4..."
curl -fSL -o "$TMP_DIR/tmux.tar.zst" \
    "$MSYS2_REPO/tmux-3.4-2-x86_64.pkg.tar.zst"
tmux_tar=$(decompress_zst "$TMP_DIR/tmux.tar.zst")
mkdir -p "$TMP_DIR/tmux"
cd "$TMP_DIR/tmux" && tar -xf "$tmux_tar" usr/bin/tmux.exe
cp -f "$TMP_DIR/tmux/usr/bin/tmux.exe" "$INSTALL_DIR/tmux.exe"
chmod +x "$INSTALL_DIR/tmux.exe"

hash -r

# 4. Ensure ~/bin is on PATH for this session
if [[ "$INSTALL_DIR" == "$HOME/bin" ]]; then
    export PATH="$HOME/bin:$PATH"
    # Persist in .bashrc if not already there
    if ! grep -q 'export PATH="$HOME/bin:$PATH"' "$HOME/.bashrc" 2>/dev/null; then
        echo 'export PATH="$HOME/bin:$PATH"' >> "$HOME/.bashrc"
        echo "    Added ~/bin to PATH in ~/.bashrc"
    fi
fi

# 5. Verify
echo ""
if "$INSTALL_DIR/tmux.exe" -V 2>/dev/null; then
    echo "==> SUCCESS: $("$INSTALL_DIR/tmux.exe" -V) installed at $INSTALL_DIR/tmux.exe"
else
    echo "==> tmux 3.4 didn't work, trying 3.5a..."
    curl -fSL -o "$TMP_DIR/tmux35.tar.zst" \
        "$MSYS2_REPO/tmux-3.5.a-2-x86_64.pkg.tar.zst"
    tmux35_tar=$(decompress_zst "$TMP_DIR/tmux35.tar.zst")
    mkdir -p "$TMP_DIR/tmux35"
    cd "$TMP_DIR/tmux35" && tar -xf "$tmux35_tar" usr/bin/tmux.exe
    cp -f "$TMP_DIR/tmux35/usr/bin/tmux.exe" "$INSTALL_DIR/tmux.exe"
    chmod +x "$INSTALL_DIR/tmux.exe"
    hash -r

    if "$INSTALL_DIR/tmux.exe" -V 2>/dev/null; then
        echo "==> SUCCESS: $("$INSTALL_DIR/tmux.exe" -V) installed at $INSTALL_DIR/tmux.exe"
    else
        echo "==> ERROR: tmux not working."
        echo "    Debug: ldd $INSTALL_DIR/tmux.exe"
        echo "    Your Git Bash msys-2.0.dll may be too old. Update Git for Windows."
        exit 1
    fi
fi
