
CSV1 = 'dev_parc_2022-05-22.csv'
CSV2 = 'penalidades_sv_2022-05-22.csv'
CSV3 = 'penalidades_vfr_2022-05-22.csv'
CSV4 = 'resarcimientos_2022-05-22.csv'

CSVa = '20220530_010130-dev_parc_2022-05-22.csv'
CSVb = '20220530_010230-penalidades_sv_2022-05-22.csv'
CSVc = '20220530_010548-penalidades_vfr_2022-05-22.csv'
CSVd = '20220530_010746-resarcimientos_2022-05-22.csv'


def compare_CSVs(CSV1, CSV2):

    with open('./Others/compare_CSVs/files_to_compare/{}'.format(CSV1), 'r') as t1, open('./Others/compare_CSVs/files_to_compare/{}'.format(CSV2), 'r') as t2:
        csv_1 = t1.readlines()
        csv_2 = t2.readlines()

    with open('./Others/compare_CSVs/diffs/diff_{}.csv'.format(CSV1.split('_2022')[0]), 'w') as out_file:
        for line in csv_2:
            if line not in csv_1:
                out_file.write(line)


compare_CSVs(CSV1, CSVa)
compare_CSVs(CSV2, CSVb)
compare_CSVs(CSV3, CSVc)
compare_CSVs(CSV4, CSVd)

compare_CSVs(CSVa, CSV1)
compare_CSVs(CSVb, CSV2)
compare_CSVs(CSVc, CSV3)
compare_CSVs(CSVd, CSV4)
