#!/usr/bin/env bash

read -r -n 1 -p "Want to clean repo [USE ONLY IN GRID] [y/n] " response
  case "${response}" in
    [yY] )
      echo ""
      while read changed_file; do
        echo "Reverting ${changed_file}"
        git checkout -- "${changed_file}"
      done < <(git diff --name-only)

      while read untracked_file; do
        echo "Removing ${untracked_file}"
        rm -r "${untracked_file}"
      done < <(git ls-files --other)
      exit 0 ;;
    *)
      echo ""
      exit 0 ;;
  esac
