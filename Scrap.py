import requests
import pandas as pd
from bs4 import BeautifulSoup

url = 'https://www.regelleistung.net/ext/data/?lang=en'
headers = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.80 '
                  'Safari/537.36',
}


def collect_data(tso_id_, data_type_):
    request_data = {
        'from': '05.02.2022',
        '_download': 'on',
        'tsoId': tso_id_,
        'dataType': data_type_,
    }

    column_list = []
    final_list = []

    response = None
    try:
        response = requests.post(url, data=request_data, headers=headers)
    except Exception:
        print('Unable to connect with new connection')

    if response is not None and response.status_code == 200:

        response_text = response.content

        soup = BeautifulSoup(response_text, "html.parser")

        # Select Table Element
        table_ele = soup.find('table', {'id': 'data-table'})

        # Find all column
        columns = table_ele.find_all('th')

        for column in columns:
            column_list.append(column.text)

        # Find all rows
        rows = table_ele.find_all('tr')
        for row in rows:
            temp = []
            all_tds = row.find_all('td')

            if len(all_tds) == 0:
                continue

            for cell in all_tds:
                cell_content = cell.text
                temp.append(cell_content)

            final_list.append(temp)

    return pd.DataFrame(final_list, columns=column_list)


tso_ids = {
    '3': 'Amprion',
    '2': 'TenneT',
    '1': 'TransnetBW',
    '6': 'Netzregelverbund',
    '11': 'IGCC',
    '-42': 'Netzregelverbund detailliert',
}
data_types = {
    'MRL': 'MR',
    'SRL': 'SCR',
    'RZ_SALDO': 'RZ_SALDO',
    'REBAP': 'REBAP',
    'ZUSATZMASSNAHMEN': 'emergency power',
    'NOTHILFE': 'emergency assistance',
}

frames = []

for tso_id in tso_ids.items():
    tso_id_key, tso_id_value = tso_id

    for data_type in data_types.items():
        data_type_key, data_type_value = data_type

        temp_df = collect_data(tso_id_key, data_type_key)
        temp_df['tso_id'] = tso_id_value
        temp_df['data_type'] = data_type_value
        frames.append(temp_df)

df_concat = pd.concat(frames)
# df_concat

df_concat.to_csv('data.csv')
