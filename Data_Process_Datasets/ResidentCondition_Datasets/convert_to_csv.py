import pandas as pd

# read the tab-separated file
df = pd.read_csv('mapped_data.txt', sep='\t', header=None, names=[
    'NTACode',
    'NTAName',
    'SevereRentBurdenVsCity',
    'NotRentStabilizedVsCity',
    '3PlusMaintenanceDeficienciesVsCity',
    'ChangeInRentsVsCity',
    'ChangeInPopulationWithBachelorsDegreesVsCity',
    'Adjacency'
])

# save as CSV
df.to_csv('cleaned_data.csv', index=False)

print("\nFirst few rows of the converted data:")
print(df.head())