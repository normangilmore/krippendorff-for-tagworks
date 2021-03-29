import numpy as np
from krippendorff import alpha

# Example from: Krippendorff, Klaus. "Content Analysis: An Introduction to Its Methodology".
# Fourth Edition. 2019. SAGE Publishing.
# Chapter 12, page 290.
# 4 observers (rows). 11 units (columns)
# np.nan is missing data (observer did not code unit)
reliability_data = np.array([
    [ 1.,  2.,  3.,  3.,  2.,  1.,  4.,  1.,  2., np.nan, np.nan],
    [ 1.,  2.,  3.,  3.,  2.,  2.,  4.,  1.,  2.,  5., np.nan],
    [np.nan,  3.,  3.,  3.,  2.,  3.,  4.,  2.,  2.,  5.,  1.],
    [ 1.,  2.,  3.,  3.,  2.,  4.,  4.,  1.,  2.,  5.,  1.]
])

if __name__ == "__main__":
    alpha_nominal = alpha(reliability_data=reliability_data,level_of_measurement='nominal')
    print(f"Nominal: {alpha_nominal}")
    assert(np.isclose(alpha_nominal, 0.743421052631579))

    alpha_interval = alpha(reliability_data=reliability_data,level_of_measurement='interval')
    print(f"Interval: {alpha_interval}")
    assert(np.isclose(alpha_interval, 0.849, atol=0.001))

    alpha_ratio = alpha(reliability_data=reliability_data,level_of_measurement='ratio')
    print(f"Ratio: {alpha_ratio}")
    assert(np.isclose(alpha_ratio, 0.797, atol=0.001))

    alpha_ordinal = alpha(reliability_data=reliability_data,level_of_measurement='ordinal')
    print(f"Ordinal: {alpha_ordinal}")
    assert(np.isclose(alpha_ordinal, 0.815, atol=0.001))
