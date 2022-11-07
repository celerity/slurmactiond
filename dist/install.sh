#!/bin/bash

set -eu -o pipefail

usage() {
    echo "Usage: $0 [--root ROOT] [--prefix PREFIX] [--user] [--dry-run] [--config] [--help]"
    echo
    echo "    --root ROOT      target filesystem root (default /)"
    echo "    --prefix PREFIX  installation path relative to root (default /usr/local)"
    echo "    --user           install into \$HOME/.local and \$HOME/.config instead"
    echo "    --config         install (potentially overwrite) config files"
    echo "    --dry-run        print installation paths, but do not actually install"
    echo "    --help           show this help"
}

SOURCE_ROOT="$(readlink -f ""$(dirname $0)/..)"

OPT_ROOT=
OPT_PREFIX=/usr/local
OPT_USER=
OPT_DRY_RUN=
OPT_CONFIG=
while [ $# -gt 0 ]; do
    case "$1" in
        --root) OPT_ROOT="$2"; shift 2;;
        --prefix) OPT_PREFIX="$2"; shift 2;;
        --user) OPT_USER=1; shift;;
        --config) OPT_CONFIG=1; shift;;
        --dry-run) OPT_DRY_RUN=1; shift;;
        --help) usage; exit 0;;
        --) shift;;
        *) echo "Unexpected argument \"$1\"" >&2; (usage) >&2; exit 1;;
    esac
done

NEEDS_ROOT=
if [ -n "$OPT_USER" ]; then
    DIR_BIN="$HOME/.local/bin"
    DIR_RES="$HOME/.local/share/slurmactiond"
    DIR_CONFIG="$HOME/.config"
    DIR_SYSTEMD="$HOME/.local/share/systemd/user"
else
    ABS_PREFIX="$OPT_ROOT$OPT_PREFIX"
    if ! [ -w "$OPT_ROOT" ] || ([ -e "$ABS_PREFIX" ] && ! [ -w "$ABS_PREFIX" ]); then
        NEEDS_ROOT=1
    fi
    DIR_BIN="$ABS_PREFIX/bin"
    DIR_RES="$ABS_PREFIX/share/slurmactiond"
    DIR_CONFIG="$OPT_ROOT/etc"
    DIR_SYSTEMD="$OPT_ROOT/lib/systemd"
fi

echo "binary path:       $DIR_BIN"
echo "config path:       $DIR_CONFIG"
echo "resource path      $DIR_RES"
echo "systemd unit path: $DIR_SYSTEMD"
echo

echo cd $SOURCE_ROOT
cd $SOURCE_ROOT

maybe() {
    echo "$@"
    if [ -z "$OPT_DRY_RUN" ]; then
        "$@"
    fi
}

maybe_sudo() {
    if [ -n "$NEEDS_ROOT" ]; then
        maybe sudo "$@"
    else
        maybe "$@"
    fi
}

maybe cargo build --release

maybe_sudo install -m 0755 -d "$DIR_BIN" "$DIR_CONFIG" "$DIR_RES" "$DIR_SYSTEMD"
maybe_sudo install -m 0755 target/release/slurmactiond "$DIR_BIN/slurmactiond"

if [ -n "$OPT_CONFIG" ]; then
    maybe_sudo install -m 0644 slurmactiond.example.service "$DIR_SYSTEMD/slurmactiond.service"
    maybe_sudo install -m 0644 slurmactiond.example.toml "$DIR_CONFIG/slurmactiond.toml"
fi

IFS=$'\n' FILES_RES=($(find res -type f -printf '%P\n'))
for FILE in "${FILES_RES[@]}"; do
    maybe_sudo install -m 0644 -D "res/$FILE" "$DIR_RES/$FILE"
done
