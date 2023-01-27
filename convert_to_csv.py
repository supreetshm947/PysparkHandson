import pandas as pd

#Read xls file
xls_file = pd.ExcelFile('./data/Top_IMDB_Movies.xlsx')

#Read the sheet in xls file
sheet1 = xls_file.parse('List')

#Save the sheet as csv file
sheet1.to_csv('Top_IMDB_Movies.xlsx.csv', sep=';')