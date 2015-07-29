#!/bin/bash -e


PERFORM_RELEASE=""


function die() {
    echo >&2 "$1"
    exit 1
}


while [[ "$1" ]]; do
    case "$1" in
        "--release")
            PERFORM_RELEASE="1"
            ;;
        *)
            if [ -z "$PACKAGE_NAME" ]; then
                PACKAGE_NAME="$1"
            elif [ -z "$RELEASE_VER" ]; then
                RELEASE_VER="$1"
            elif [ -z "$POST_RELEASE_VER" ]; then
                POST_RELEASE_VER="$1"
            else
                die "Unexpected positional parameter: $1"
            fi
            ;;
    esac
    shift
done

if [ -z "$PACKAGE_NAME" ]; then
    die "Package name must be provided."
elif [ -z "$RELEASE_VER" ]; then
    die "Release version number must be provided."
fi

SHORT_VER="${RELEASE_VER%.*}"

if [ -z "$POST_RELEASE_VER" ]; then
    POINT_NUM="${RELEASE_VER##*.}"
    NEXT_POINT_NUM=$((${POINT_NUM} + 1))
    POST_RELEASE_VER="${SHORT_VER}.${NEXT_POINT_NUM}a0"
fi

RELEASE_BRANCH="release/${RELEASE_VER%.*}.x"

git checkout develop
if ! git diff-index --quiet HEAD; then
    echo >&2 "Uncommitted changes found, aborting."
    exit 1
fi

BUMP_VERSION="./bump-version.sh"
if [ ! -x "$BUMP_VERSION" ]; then
    BUMP_VERSION="./utils/bump-version.sh"
fi
if [ ! -x "$BUMP_VERSION" ]; then
    die "bump-version.sh not found."
fi

function showvar() {
    eval echo "$1=[\$$1]"
}

showvar "PACKAGE_NAME"
showvar "RELEASE_VER"
showvar "POST_RELEASE_VER"
showvar "RELEASE_BRANCH"
showvar "PERFORM_RELEASE"
showvar "BUMP_VERSION"

STAGE="start"

# set -x

# Make sure all our branches are up to date.
echo -e "\nUpdating branches...\n"
git checkout develop > /dev/null
git pull
git checkout master > /dev/null
git merge origin/master
git checkout ${RELEASE_BRANCH} > /dev/null
git merge origin/${RELEASE_BRANCH}
git checkout develop > /dev/null


echo -e "\nChecking where we are...\n"

$BUMP_VERSION "${POST_RELEASE_VER}"
git update-index --refresh || true
if git diff-files --quiet; then
    echo "We're already at the post-release version."
    STAGE="postreleasever"
fi

$BUMP_VERSION "${RELEASE_VER}"
git update-index --refresh || true
if git diff-files --quiet; then
    echo "We're already at the release version."
    STAGE="releasever"
fi
git checkout -- .

echo $STAGE
if [ "$STAGE" == "start" ]; then
    # Set the version number in develop.
    echo -e "\nSetting release version number...\n"
    git checkout develop > /dev/null
    $BUMP_VERSION "${RELEASE_VER}"
    git add .
    git commit -m "Release version ${RELEASE_VER}."
    STAGE="releasever"
fi

if [ "$STAGE" = "releasever" ]; then
    # Update other local branches and tag.
    echo -e "\nUpdating release and master branches...\n"
    git checkout "${RELEASE_BRANCH}"
    git merge --ff-only develop
    git checkout master
    git merge --ff-only develop
    git tag "${PACKAGE_NAME}-${RELEASE_VER}"

    # Build release packages.
    # We do this before pushing to keep everything local as long as possible.
    echo -e "\nBuilding packages...\n"
    rm dist/* || true
    python setup.py sdist bdist_wheel
    STAGE="postreleasever"
fi

if [ -n "${PERFORM_RELEASE}" ]; then
    # Now we push, register, and upload.
    git push origin develop master "${RELEASE_BRANCH}"
    git push --tags
    python setup.py register
    twine upload dist/*

    # Bump version post-release in develop, push, and we're done.
    git checkout develop
    $BUMP_VERSION "${POST_RELEASE_VER}"
    git add .
    git commit -m "Bump version to ${POST_RELEASE_VER} post-release."
    git push origin develop
else
    echo "Not performing release."
fi
