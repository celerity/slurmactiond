#!/bin/bash

set -eu -o pipefail

usage() {
    echo "Usage: $0 [--root ROOT] [--prefix PREFIX] [--user] [--dry-run] [--help]" >&2
    exit 1
}

SOURCE_ROOT="$(readlink -f ""$(dirname $0)/..)"

OPT_ROOT=
OPT_PREFIX=/usr/local
OPT_USER=
OPT_DRY_RUN=
while [ $# -gt 0 ]; do
    case "$1" in
        --root) OPT_ROOT="$2"; shift 2;;
        --prefix) OPT_PREFIX="$2"; shift 2;;
        --user) OPT_USER=1; shift;;
        --dry-run) OPT_DRY_RUN=1; shift;;
        --help) (usage) || exit 0;;
        --) shift;;
        *) echo "Unexpected argument \"$1\"" >&2; usage;;
    esac
done

if [ -n "$OPT_USER" ]; then
    DIR_BIN="$HOME/.local/bin"
    DIR_CONFIG="$HOME/.config"
    DIR_SYSTEMD="$HOME/.local/share/systemd/user"
else
    DIR_BIN="$OPT_ROOT$OPT_PREFIX/bin"
    DIR_CONFIG="$OPT_ROOT/etc"
    DIR_SYSTEMD="$OPT_ROOT/lib/systemd"
fi

echo "binary path:       $DIR_BIN"
echo "config path:       $DIR_CONFIG"
echo "systemd unit path: $DIR_SYSTEMD"
echo

maybe() {
    echo "$@"
    if [ -z "$OPT_DRY_RUN" ]; then
        "$@"
    fi
}

maybe cd $SOURCE_ROOT
maybe cargo build --release
maybe install -m 0755 -d "$DIR_BIN" "$DIR_CONFIG" "$DIR_SYSTEMD"
maybe install -m 0755 target/release/slurmactiond "$DIR_BIN/slurmactiond"
maybe install -m 0644 slurmactiond.example.service "$DIR_SYSTEMD/slurmactiond.service"
maybe install -m 0644 slurmactiond.example.toml "$DIR_CONFIG/slurmactiond.toml"
