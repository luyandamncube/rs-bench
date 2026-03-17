import os
import sys
import re


def strip_comment(line: str) -> str:
    """
    Remove inline comments from a tree line.
    Only strips comments that start with whitespace + #,
    so names containing '#' are less likely to break.
    """
    return re.sub(r"\s+#.*$", "", line).rstrip()


def is_tree_separator(line: str) -> bool:
    """
    Ignore visual spacer lines like:
        │
        │   
    """
    stripped = line.strip()
    return stripped in {"│", ""}


def get_level(line: str) -> int:
    """
    Determine nesting level from tree format.
    Count occurrences of either '│   ' or '    ' groups.
    """
    prefix_match = re.match(r"^((?:│   |    )*)", line)
    if not prefix_match:
        return 0

    prefix = prefix_match.group(1)
    return len(prefix) // 4


def extract_name(line: str) -> str:
    """
    Remove tree drawing characters and return actual file/folder name.
    """
    line = re.sub(r"^((?:│   |    )*)?(├── |└── )", "", line)
    return line.strip()


def scaffold_from_tree(tree_text: str):
    stack = []
    root_set = False

    for raw_line in tree_text.splitlines():
        line = strip_comment(raw_line)

        if is_tree_separator(line):
            continue

        # Root folder: only allow this once, on the first real line
        if not root_set and "├──" not in line and "└──" not in line:
            root_name = line.strip().rstrip("/")
            if not root_name:
                continue

            stack = [root_name]
            os.makedirs(root_name, exist_ok=True)
            print(f"Created dir: {root_name}")
            root_set = True
            continue

        # Ignore any malformed non-root line that doesn't contain tree markers
        if "├──" not in line and "└──" not in line:
            print(f"Skipping unrecognized line: {raw_line!r}")
            continue

        level = get_level(line)
        name = extract_name(line)

        if not name:
            continue

        is_dir = name.endswith("/")
        clean_name = name.rstrip("/")

        # stack[0] = root, then level 0 items are direct children of root
        stack = stack[: level + 1]
        stack.append(clean_name)

        path = os.path.join(*stack)

        if is_dir:
            os.makedirs(path, exist_ok=True)
            print(f"Created dir: {path}")
        else:
            parent = os.path.dirname(path)
            if parent:
                os.makedirs(parent, exist_ok=True)
            with open(path, "a", encoding="utf-8"):
                pass
            print(f"Created file: {path}")

    print("\n✅ Scaffold created correctly.")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python scaffold_from_tree.py DIRTREE.txt")
        sys.exit(1)

    with open(sys.argv[1], "r", encoding="utf-8") as f:
        tree = f.read()

    scaffold_from_tree(tree)