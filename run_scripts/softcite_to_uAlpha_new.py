import os
from validation.hl_to_reliability import split_highlighter

if __name__ == "__main__":
    main_dir = "/Users/ng/Documents/CLIENTS/Thusly Inc/Customers/UT Austin-Howison/"
    in_dir_1E = os.path.join(
        main_dir,
        "Pipeline-1-SoftCite Mentions-Oct 2020/Pipeline-1E-Validation/Output data"
    )
    highlighter_1E = os.path.join(
        in_dir_1E,
        "Pipeline-1E-SoftciteI_fromJSON-MTurk-2020-10-30T1440-Highlighter.csv.gz"
    )
    out_dir_1E = os.path.join(in_dir_1E,"uAlpha-format")
    split_highlighter(highlighter_1E, out_dir_1E, "Pipeline-1E-uAlpha-{}")

    in_dir_3E = os.path.join(
        main_dir,
        "Pipeline-3-SoftCite-HEP-Mentions/Output data"
    )
    highlighter_3E = os.path.join(
        in_dir_3E,
        "Pipeline-3E-HEP-Mentions-MTurk-2021-01-10T0211-Highlighter.csv.gz"
    )
    out_dir_3E = os.path.join(in_dir_3E,"uAlpha-format")


    split_highlighter(highlighter_3E, out_dir_3E, "Pipeline-3E-uAlpha-{}")
