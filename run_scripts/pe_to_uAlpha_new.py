import os
import fnmatch
from validation.hl_to_reliability import split_highlighter

if __name__ == "__main__":
    in_dir = "/Users/ng/Documents/CLIENTS/Nicks Projects/Public Editor/PE-data"
    out_dir = os.path.join(in_dir, "uAlpha-format")
    os.makedirs(out_dir, exist_ok=True)
    for in_filename in fnmatch.filter(os.listdir(in_dir), '*Highlighter.csv'):
        in_path = os.path.join(in_dir, in_filename)
        batch_name = in_filename.split("-")[0] + "-uAlpha-{}"
        split_highlighter(in_path, out_dir, batch_name)
