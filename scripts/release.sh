#!/bin/bash
# Isolated, exact-source releases. No caller files or refs are changed.
set -euo pipefail
export GOWORK=off
fail() { echo "release: $*" >&2; exit 1; }
for key in GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_COMMON_DIR GIT_OBJECT_DIRECTORY GIT_ALTERNATE_OBJECT_DIRECTORIES GIT_NAMESPACE GIT_CONFIG; do
  [[ -z ${!key:-} ]] || fail "unsupported repository selector: $key"
done
repo=$(git rev-parse --show-toplevel)
cd "$repo"
common=$(git rev-parse --git-common-dir)
[[ "$common" = /* ]] || common="$repo/$common"
base="$common/library-releases"
[[ ! -L "$base" ]] || fail 'state directory must not be a symlink'
mkdir -p "$base"
for legacy in "$common"/*-releases; do
  [[ "$legacy" == "$base" ]] && continue
  [[ ! -e "$legacy/active" && ! -e "$legacy/shell-active" ]] || fail "legacy active release at $legacy; inspect/finish with the previous tooling revision"
done
active="$base/active"
lock="$base/lock"
mkdir "$lock" 2>/dev/null || fail "another release is running; inspect stale lock manually: $lock"
trap 'rmdir "$lock" 2>/dev/null || true' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
put() {
  [[ ! -L "$active/$1" && ! -L "$active/$1.tmp" ]] || fail "record must not be a symlink: $1"
  printf '%s\n' "$2" > "$active/$1.tmp"; mv "$active/$1.tmp" "$active/$1"
}
get() { [[ -f "$active/$1" && ! -L "$active/$1" ]] || fail "invalid record $1"; cat "$active/$1"; }
clean() { [[ -z $(git status --porcelain --untracked-files=no) ]] || fail 'tracked files and index must be clean'; }
main_source() {
  [[ $(git symbolic-ref --quiet HEAD) == refs/heads/main ]] || fail 'release requires local main checkout'
  git merge-base --is-ancestor "$source" refs/heads/main || fail 'source must belong to local main'
}
remote=$(git remote get-url --push --all origin)
[[ -n "$remote" && "$remote" != *$'\n'* ]] || fail 'exactly one origin push destination required'
case "$remote" in /*|*:* ) ;; *) remote="$repo/$remote";; esac
remote_main() {
  local rows
  rows=$(git ls-remote --refs -- "$remote" refs/heads/main) || return 1
  remote_oid=$(printf '%s\n' "$rows" | awk '$2=="refs/heads/main" {print $1}')
  [[ "$remote_oid" =~ ^[0-9a-f]{40}$ ]] || return 1
  git -C "$checkout" fetch --quiet --no-tags -- "$remote" "$remote_oid" || return 1
}
fast_forward() {
  remote_main || fail 'cannot read existing remote main'
  git -C "$checkout" merge-base --is-ancestor "$remote_oid" "$source" || fail 'remote main cannot fast-forward to selected source'
  put remote-main "$remote_oid"
}
observe() {
  local rows ref oid found count=0 total=0 delivered=0
  if ! remote_main; then put status unknown; return 1; fi
  if git -C "$checkout" merge-base --is-ancestor "$source" "$remote_oid"; then delivered=1; fi
  if ! rows=$(git ls-remote --refs -- "$remote" 'refs/tags/*'); then put status unknown; return 1; fi
  while IFS=' ' read -r ref oid; do
    [[ -n "$ref" ]] || continue
    total=$((total+1))
    found=$(printf '%s\n' "$rows" | awk -v r="$ref" '$2==r {print $1}')
    if [[ -n "$found" && "$found" != "$oid" ]]; then put status collision; return 1; fi
    [[ -z "$found" ]] || count=$((count+1))
  done < "$active/refs"
  if (( count == total && delivered == 1 )); then put status complete
  elif (( count == 0 )); then put status none
  else put status partial; fi
}
load() {
  [[ -d "$active" && ! -L "$active" ]] || fail 'no active shell release'
  [[ $(get format) == 3 ]] || fail 'unsupported active record format; recover using its original tooling revision'
  [[ $(get branch) == refs/heads/main ]] || fail 'release branch record changed'
  [[ $(get remote) == "$remote" ]] || fail 'origin destination changed'
  source=$(get source); version=$(get version); kind=$(get kind)
  [[ "$source" =~ ^[0-9a-f]{40}$ && "$version" =~ ^v[01]\.[0-9]+\.[0-9]+$ ]] || fail 'invalid source/version record'
  [[ "$kind" == patch || "$kind" == break ]] || fail 'invalid release kind'
  checkout="$active/checkout"
  [[ -d "$checkout/.git" && ! -L "$checkout" && ! -L "$checkout/.git" ]] || fail 'invalid candidate checkout'
}
validate() {
  local candidate parent changed path expected module ref allowed
  for path in modules refs files module-paths; do get "$path" >/dev/null; done
  [[ "$(cd "$checkout" && unset MAKEFLAGS MAKEOVERRIDES MODULES && make --no-print-directory -s modules)" == "$(cat "$active/modules")" ]] || fail 'module inventory record changed'
  candidate=$(get candidate)
  [[ "$candidate" =~ ^[0-9a-f]{40}$ && $(git -C "$checkout" rev-parse HEAD) == "$candidate" ]] || fail 'candidate identity changed'
  [[ -z $(git -C "$checkout" status --porcelain --untracked-files=no) ]] || fail 'candidate is dirty'
  if [[ "$candidate" != "$source" ]]; then
    parent=$(git -C "$checkout" rev-list --parents -n 1 HEAD)
    [[ "$parent" == "$candidate $source" ]] || fail 'candidate ancestry changed'
  fi
  expected=$(while IFS= read -r module; do
    ref="refs/tags/$version"; [[ "$module" == . ]] || ref="refs/tags/$module/$version"
    printf '%s %s\n' "$ref" "$candidate"
  done < "$active/modules")
  [[ "$(cat "$active/refs")" == "$expected" ]] || fail 'ref inventory record changed'
  allowed=$(while IFS= read -r module; do
    if [[ "$module" == . ]]; then printf 'go.mod\ngo.sum\n'; else printf '%s/go.mod\n%s/go.sum\n' "$module" "$module"; fi
  done < "$active/modules")
  [[ "$(cat "$active/files")" == "$allowed" ]] || fail 'manifest allowlist record changed'
  changed=$(git -C "$checkout" diff --name-only "$source" "$candidate")
  while IFS= read -r path; do
    [[ -z "$path" ]] || grep -Fxq -- "$path" "$active/files" || fail "unexpected candidate file: $path"
  done <<< "$changed"
  while IFS=' ' read -r ref oid; do
    [[ "$oid" == "$candidate" ]] || fail 'ref record changed'
    [[ $(git -C "$checkout" rev-parse "$ref") == "$candidate" ]] || fail 'candidate tag changed'
  done < "$active/refs"
}
seal() {
  local module ref oid existing
  : > "$active/refs"
  while IFS= read -r module; do
    ref="refs/tags/$version"; [[ "$module" == . ]] || ref="refs/tags/$module/$version"
    oid=$(get candidate)
    existing=$(git -C "$checkout" show-ref --verify --hash "$ref" 2>/dev/null || true)
    [[ -z "$existing" || "$existing" == "$oid" ]] || fail "isolated tag collision: $ref"
    [[ -n "$existing" ]] || git -C "$checkout" update-ref "$ref" "$oid" "$(printf '%040d' 0)"
    printf '%s %s\n' "$ref" "$oid" >> "$active/refs"
  done < "$active/modules"
}
prepare() {
  local module modpath dependency canonical file
  # Rebuild incomplete preparation from the same immutable source in the owned checkout.
  git -C "$checkout" reset --hard --quiet "$source"
  git -C "$checkout" clean -fdq
  (cd "$checkout" && make lint && make test && make test-integration && make test-e2e)
  : > "$active/files"
  while IFS= read -r module; do
    [[ "$module" == . || "$module" =~ ^[a-zA-Z0-9_-]+(/[a-zA-Z0-9_-]+)*$ ]] || fail "invalid module: $module"
    [[ "$module" != *..* ]] || fail 'invalid module traversal'
    modpath="$checkout/$module/go.mod"
    [[ -f "$modpath" && ! -L "$modpath" ]] || fail "invalid manifest: $module"
    canonical=$(cd "$checkout/$module" && go mod edit -print)
    while IFS= read -r dependency; do
      [[ -n "$dependency" ]] || continue
      if printf '%s\n' "$canonical" | awk -v p="$dependency" '$1=="require" && $2==p {found=1} $1=="require" && $2=="(" {block=1;next} block && $1==")" {block=0} block && $1==p {found=1} END {exit !found}'; then
        (cd "$checkout/$module" && go mod edit "-require=$dependency@$version")
      fi
      (cd "$checkout/$module" && go mod edit "-dropreplace=$dependency")
    done < "$active/module-paths"
    file=go.mod; [[ "$module" == . ]] || file="$module/go.mod"
    printf '%s\n' "$file" >> "$active/files"
    if [[ "$module" == . ]]; then printf 'go.sum\n' >> "$active/files"; else printf '%s/go.sum\n' "$module" >> "$active/files"; fi
  done < "$active/modules"
  while IFS= read -r file; do
    [[ ! -L "$checkout/$file" ]] || fail "manifest must not be a symlink: $file"
    if [[ -f "$checkout/$file" ]]; then git -C "$checkout" add -- "$file"; fi
  done < "$active/files"
  if ! git -C "$checkout" diff --cached --quiet; then git -C "$checkout" commit --quiet -m "chore: release $version"; fi
  put candidate "$(git -C "$checkout" rev-parse HEAD)"
  seal
  put phase prepared
}
operation=${1:-}
case "$operation" in
 inspect)
  load
  if [[ -f "$active/refs" ]]; then observe || true; fi
  for field in format branch remote-main source version kind phase candidate status; do [[ ! -f "$active/$field" ]] || printf '%s: %s\n' "$field" "$(get "$field")"; done
  exit 0;;
 finish)
  load; clean; validate; observe || fail 'cannot confirm publication'
  [[ $(get status) == complete ]] || fail 'publication is not complete'
  mkdir -p "$base/history"; mv "$active" "$base/history/$version-$(get candidate)-shell"; exit 0;;
 resume)
  load; clean; main_source
  [[ $(get status) != unknown && $(get status) != collision ]] || fail 'run inspect before resuming an unknown/colliding publication';;
 patch|break)
  clean
  [[ -z ${2:-} || "$2" =~ ^[0-9a-f]{40}$ ]] || fail 'RELEASE_SOURCE must be a full commit SHA'
  requested=$(git rev-parse --verify "${2:-HEAD}^{commit}")
  source="$requested"; main_source
  if [[ -e "$active" ]]; then
    load
    [[ "$source" == "$requested" && "$kind" == "$operation" ]] || fail 'finish or resume the existing candidate first'
    [[ $(get status) != unknown && $(get status) != collision ]] || fail 'run inspect before retry'
  else
    rows=$(git ls-remote --refs -- "$remote" 'refs/tags/*')
    latest=$(printf '%s\n' "$rows" | sed -n 's|.*refs/tags/v\([0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*\)$|\1|p' | sort -t. -k1,1n -k2,2n -k3,3n | tail -n 1)
    IFS=. read -r major minor patch <<< "${latest:-0.0.0}"
    if [[ "$operation" == patch ]]; then patch=$((patch+1)); elif (( major == 0 )); then minor=$((minor+1)); patch=0; else fail 'v2+ requires a semantic import-version migration'; fi
    version="v$major.$minor.$patch"; source="$requested"; kind="$operation"
    mkdir "$active"
    put format 3; put branch refs/heads/main
    put source "$source"; put version "$version"; put kind "$kind"; put remote "$remote"; put phase preparing; put status none
    checkout="$active/checkout"
    git init --quiet "$checkout"
    git -C "$checkout" fetch --quiet --no-tags "$repo" "$source"
    git -C "$checkout" checkout --quiet --detach "$source"
    git -C "$checkout" config core.hooksPath /dev/null
    for key in user.name user.email user.signingkey commit.gpgsign gpg.format gpg.program gpg.ssh.program; do
      value=$(git config --get "$key" || true); [[ -z "$value" ]] || git -C "$checkout" config "$key" "$value"
    done
    (cd "$checkout" && unset MAKEFLAGS MAKEOVERRIDES MODULES && make --no-print-directory -s modules) > "$active/modules"
    [[ $(head -n 1 "$active/modules") == . && $(sort "$active/modules" | uniq -d | wc -l | tr -d ' ') == 0 ]] || fail 'invalid release module inventory'
    : > "$active/module-paths"
    while IFS= read -r module; do
      [[ "$module" == . || "$module" =~ ^[a-zA-Z0-9_-]+(/[a-zA-Z0-9_-]+)*$ ]] || fail 'invalid module directory'
      [[ "$module" != *..* ]] || fail 'invalid module traversal'
      file=go.mod; [[ "$module" == . ]] || file="$module/go.mod"
      mode=$(git -C "$checkout" ls-tree "$source" -- "$file" | awk '{print $1}')
      [[ "$mode" == 100644 || "$mode" == 100755 ]] || fail "manifest must be a tracked regular file: $file"
      path=$(cd "$checkout/$module" && go list -m -f '{{.Path}}')
      [[ "$module" == . || "$path" == "$(head -n 1 "$active/module-paths")/$module" ]] || fail "unexpected module path: $path"
      printf '%s\n' "$path" >> "$active/module-paths"
      ref="refs/tags/$version"; [[ "$module" == . ]] || ref="refs/tags/$module/$version"
      [[ -z $(git show-ref --verify "$ref" 2>/dev/null || true) ]] || fail "local tag collision: $ref"
      [[ -z $(printf '%s\n' "$rows" | awk -v r="$ref" '$2==r') ]] || fail "remote tag collision: $ref"
    done < "$active/modules"
    fast_forward
  fi;;
 *) fail 'usage: release.sh patch|break [SOURCE] | inspect|resume|finish';;
esac
if [[ $(get phase) != prepared ]]; then
  if [[ -f "$active/candidate" ]]; then
    [[ $(git -C "$checkout" rev-parse HEAD) == "$(get candidate)" ]] || fail 'interrupted candidate identity changed'
    [[ -z $(git -C "$checkout" status --porcelain --untracked-files=no) ]] || fail 'interrupted candidate is dirty'
    seal; validate; put phase prepared
  else prepare; fi
fi
validate
observe || fail "publication $(get status); inspect before retry"
if [[ $(get status) != complete ]]; then
  fast_forward
  printf 'Source: %s\nCandidate: %s\nVersion: %s\nDestination: %s\n' "$source" "$(get candidate)" "$version" "$remote"
  printf 'refs/heads/main %s\n' "$source"
  cat "$active/refs"
  read -r -p 'Publish these exact refs? [y/N] ' answer
  [[ "$answer" == y || "$answer" == Y ]] || fail 'aborted; candidate retained'
  fast_forward
  refs=("$source:refs/heads/main")
  while IFS=' ' read -r ref oid; do refs+=("$ref:$ref"); done < "$active/refs"
  put status unknown
  git -C "$checkout" push --atomic -- "$remote" "${refs[@]}" || true
  observe || fail 'publication unknown/collision; run inspect'
  [[ $(get status) == complete ]] || fail "publication $(get status); candidate retained"
fi
printf 'Published and verified remote refs for %s (%s). Run release.sh finish to archive the record.\n' "$version" "$(get candidate)"
