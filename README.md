# krippendorff-for-tagworks
Python utilities for calculating Krippendorff alpha on TagWorks' output data.

TagWorks has two types of projects, Highlighters and Data Hunts.

Each project exports the task run data in CSV data files.

In addition, Data Hunts have an associated Schema that can be exported to CSV.

There is a separate utility to calculate Krippendorff for each project type:

* hl_to_reliability.py
* dh_to_reliability.py

First install the Krippendorff library specified in `requirements.txt`

Probably you want to install it in a virtualenv, to avoid possible
conflicts with other software you may be using.

`pip install -r requirements.txt`

Export and download the output data file for a project, which will have a
filename like `MyProject-2021-03-29T1811-HighlighterByCase.csv.gz`

Then
`python3 reliability/hl_to_reliability.py --input MyProject-2021-03-29T1811-HighlighterByCase.csv.gz`

Note that the file does not need to be gunzipped to process - the utility will detect the
extension .gz and decompress if needed.

Data Hunts are similar, but require the Data Hunt schema file as well as the output data.
`python3 reliability/dh_to_reliability.py --schema MyProject-Schema.csv.gz --input MyProject-2021-03-29T1811-HighlighterByCase.csv.gz`
