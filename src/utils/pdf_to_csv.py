import tabula
import os

cur_dir = os.path.abspath('.')
pdf_dir = '../../pdf'
csv_dir = '../../csv'

files = os.listdir(pdf_dir)
for file in files:
    name = file.split('.')[0]
    pdf_path = os.path.join(cur_dir, pdf_dir, file)
    csv_path = os.path.join(cur_dir, csv_dir, f'{name}.csv')
    try:
        tabula.convert_into(pdf_path, csv_path, output_format="csv", lattice=True, relative_area=True, pages='all')
    except KeyboardInterrupt:
        break
    except:
        # os.remove(pdf_path)
        print(pdf_path)
