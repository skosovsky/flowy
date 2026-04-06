#!/bin/bash
set -e # Stop script on any error

RELEASE_TYPE=$1
MODULES=$2

if [ ! -f "go.mod" ]; then
    echo "Error: go.mod not found in the current directory. Run script from the repo root."
    exit 1
fi

ROOT_MODULE=$(grep -m 1 '^module' go.mod | awk '{print $2}')

if [[ "$RELEASE_TYPE" != "break" && "$RELEASE_TYPE" != "patch" ]]; then
    echo "Usage: make release-patch OR make release-break"
    exit 1
fi

if [ -z "$ROOT_MODULE" ]; then
    echo "Error: Could not determine module path from go.mod"
    exit 1
fi

REPO_PREFIX=$(echo "$ROOT_MODULE" | sed 's/\//\\\//g')

if [ -z "$MODULES" ]; then
    echo "Error: MODULES is empty. Make sure Makefile is passing it correctly."
    exit 1
fi

# Ensure there are no uncommitted changes
git update-index -q --refresh
if ! git diff-index --quiet HEAD --; then
    echo "Error: You have uncommitted changes. Please commit or stash them first."
    exit 1
fi

# 1. Fetch tags and calculate version
git fetch --tags --quiet
LATEST_TAG=$(git tag -l "v*" | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' | sort -V | tail -n 1)

if [ -z "$LATEST_TAG" ]; then
    LATEST_TAG="v0.0.0"
fi

VERSION_NO_V=${LATEST_TAG#v}
IFS='.' read -r MAJOR MINOR PATCH <<< "$VERSION_NO_V"

if [ "$RELEASE_TYPE" == "break" ]; then
    if [ "$MAJOR" -eq 0 ]; then
        MINOR=$((MINOR + 1))
        PATCH=0
    else
        MAJOR=$((MAJOR + 1))
        MINOR=0
        PATCH=0
    fi
elif [ "$RELEASE_TYPE" == "patch" ]; then
    PATCH=$((PATCH + 1))
fi

NEW_VERSION="v${MAJOR}.${MINOR}.${PATCH}"

# 3. User confirmation
echo "========================================"
echo "Current version: $LATEST_TAG"
echo "New version:     $NEW_VERSION ($RELEASE_TYPE)"
echo "========================================"
read -p "Proceed with release $NEW_VERSION? [y/N] " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 1
fi

echo "🚀 Starting release train for $NEW_VERSION..."

CURRENT_BRANCH=$(git branch --show-current)
echo "🔀 Detaching HEAD from $CURRENT_BRANCH to keep history clean..."
git checkout --detach HEAD --quiet

# 4. Update all go.mod files
echo "📦 Updating go.mod files..."
for dir in $MODULES; do
    modfile="$dir/go.mod"
    sed -i '' "/$REPO_PREFIX/s/ v0.0.0/ $NEW_VERSION/g" "$modfile"
    sed -i '' "/$REPO_PREFIX.*=>/d" "$modfile"
    go mod edit -fmt "$modfile"
done

# 5. Create the release commit
echo "💾 Committing release state (detached)..."
git add .

if ! git diff --cached --quiet; then
    git commit -m "chore: release $NEW_VERSION" --quiet
else
    echo "  ℹ️ No changes in go.mod. Will tag the current state directly."
fi

# 6. Tag the root and all submodules
echo "🏷️ Tagging root and submodules..."
git tag "$NEW_VERSION"

for dir in $MODULES; do
    if [ "$dir" != "." ]; then
        clean_dir=${dir#./}
        git tag "$clean_dir/$NEW_VERSION"
    fi
done

# 7. Push ONLY tags to GitHub
echo "☁️ Pushing tags to GitHub..."
git push origin --tags

# 8. Return to normal state
echo "⏪ Returning to $CURRENT_BRANCH..."
git checkout "$CURRENT_BRANCH" --quiet

echo "✅ Release $NEW_VERSION completed successfully! History is clean."
