#!/usr/bin/env bash
set -euo pipefail

root="$(git rev-parse --show-toplevel)"
cd "$root"

grep -Eq '^env-check:[[:space:]]*$' .just/env.just
if grep -Eq '^env-check:.*_env-dec' .just/env.just; then
  echo 'env-check must not depend on the plaintext-directory bootstrap' >&2
  exit 1
fi
test ! -e env/dec

trap_dir="$(mktemp -d)"
marker="$trap_dir/ores-sops-was-invoked"
cat > "$trap_dir/ores-sops" <<'TRAP'
#!/usr/bin/env bash
set -euo pipefail
printf 'invoked\n' > "$ORES_SOPS_TRAP_MARKER"
exit 97
TRAP
chmod 0755 "$trap_dir/ores-sops"

env \
  -u SOPS_AGE_KEY \
  -u SOPS_AGE_KEY_FILE \
  PATH="$trap_dir:$PATH" \
  ORES_SOPS_TRAP_MARKER="$marker" \
  just env-check

test ! -e "$marker"
test ! -e env/dec
printf 'keyless env-check boundary: PASS\n'
