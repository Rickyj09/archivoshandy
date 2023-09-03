from flask import Flask, render_template, request, send_file, make_response, jsonify
import pandas as pd
import io
from datetime import datetime, timedelta
import datetime 


app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/process_files', methods=['POST'])
def process_files():
    file1 = request.files['file1']
    file2 = request.files['file2']

    # Leer los archivo Excel
    df1 = pd.read_excel(file1, sheet_name='output') 
    #df1 = pd.read_excel(file1, header=None, names=['customer_id', 'customer_name','service_category'])

    #Leer el archivo csv
    
    df2 = pd.read_csv(file2)
    print(df1.columns)
    print(df2.columns)


     # Obtener la fecha de fin de mes del mes anterior
    #today = datetime.date.today()
    today = datetime.date.today()
    first_day_of_month = today.replace(day=1)
    end_of_last_month = first_day_of_month - datetime.timedelta(days=1)

    # Obtener el primer día del mes en curso
    first_day_of_current_month = today.replace(day=1)

  

    # Crear un diccionario de reemplazo para las categorías
    replacements = {
    'Intercontinental': 'GLOBAL',
    'Global': 'GLOBAL',
    ' ': 'GLOBAL',
    'North America': 'NORTHCENTRALUS',
    'US East': 'EASTUS',
    'US East 2': 'EASTUS',
    'US South Central': 'SOUTHCENTRALUS',
    'US West': 'EASTUS'
    }
    # Reemplazar valores vacíos por "Global" en la columna service_region
    df1['service_region'] = df1['service_region'].fillna('GLOBAL')

    # Reemplazar valores vacíos en el campo service_unit en df1 por "1 Hour"
    df1['service_unit'] = df1['service_unit'].fillna('1 Hour')


    # Lista de nombres de columnas a quitar
    columns_to_drop = ['resource_name']



    # Quitar las columnas especificadas
    df1 = df1.drop(columns=columns_to_drop)
    # Aplicar el reemplazo en la columna service_region
    df1['service_region'] = df1['service_region'].replace(replacements)

    # poner formato fecha
    df1['usage'] = pd.to_datetime(df1['usage'], format='%m-%d-%Y').dt.date
    
    # Quitar comillas de la columna B
    df1["customer_name"] = df1["customer_name"].str.replace('"', '')

    # Crear un diccionario de mapeo
    product_mapping = {
    'Reserved VM Instance  Standard_D2s_v3  US East  3 Years': '3 Year Reservation',
    'Reserved VM Instance  Standard_D2s_v3  US East  2 Years': '2 Year Reservation',
    'Reserved VM Instance  Standard_D2s_v3  US East  1 Year': '1 Year Reservation',
    'Reserved VM Instance  Standard_D4s_v3  US East  3 Years': '3 Year Reservation',
    'Reserved VM Instance  Standard_D4s_v3  US East  2 Years': '2 Year Reservation',
    'Reserved VM Instance  Standard_D4s_v3  US East  1 Year': '1 Year Reservation',
    'Reserved VM Instance  Standard_D8s_v3  US East  3 Years': '3 Year Reservation',
    'Reserved VM Instance  Standard_D8s_v3  US East  2 Years': '2 Year Reservation',
    'Reserved VM Instance  Standard_D8s_v3  US East  1 Year': '1 Year Reservation'
    }

# Realizar transformaciones en df1
    df1['service_category'] = df1.apply(lambda row: product_mapping.get(row['service_name'], row['service_category']), axis=1)
    df1['service_sub_category'] = df1.apply(lambda row: product_mapping.get(row['service_name'], row['service_sub_category']), axis=1)


    # Renombrar las columnas
    df1 = df1.rename(columns={
        'customer_id': 'CustomerId',
        'customer_name': 'CustomerName',
        'usage': 'UsageDate',
        'service_category': 'MeterType',
        'service_sub_category': 'MeterSubCategory',
        'service_region': 'ResourceLocation',
        'resource_group': 'ResourceGroup',
        'service_unit': 'UnitType',
        'quantity': 'Quantity',
        'total': 'PVP Total'
    })


    # Limpiar los valores en la columna "Customer Name"
    df2['Customer Name'] = df2['Customer Name'].replace({"HandytecMobi S.A. (Diego Montúfar)": "handytec.mobi"})

   # Limpiar los valores en la columna "service_category"
    df2['Customer Name'] = df2['Customer Name'].str.split('(', expand=True)[0].str.strip()

  

# Añadir una columna a df2 con la fecha de fin de mes del mes anterior
    df2['End of Last Month'] = end_of_last_month

# Modificar los valores en la columna "Product Name"
    df2.loc[df2['Product Name'].str.contains('Tax of type', case=False), 'Product Name'] = 'Tax'

# Cambiar el valor del campo ResourceLocation en df2 a "GLOBAL"
    df2['ResourceLocation'] = 'GLOBAL'

# Cambiar el valor del campo ResourceGroup en df2
    df2.loc[df2['Customer Name'] == 'handytec.mobi', 'ResourceGroup'] = 'AWS'
    df2.loc[df2['Customer Name'] != 'handytec.mobi', 'ResourceGroup'] = 'GENERAL'


# Cambiar el valor del campo total en df2
    df2.loc[df2['Customer Name'] == 'handytec.mobi', 'total'] = df2['Seller Cost (USD)']
    df2.loc[df2['Customer Name'] != 'handytec.mobi', 'total'] = df2['Customer Cost (USD)']

# Duplicar la columna Product Name y llamarla Product Name_1 en df1
    df2['Product Name_1'] = df2['Product Name']


 # Renombrar columnas del archivo CSV
    df2 = df2.rename(columns={
        'Cloud Account Number': 'CustomerId',
        'Customer Name': 'CustomerName',
        'End of Last Month': 'UsageDate',
        'Product Name': 'MeterType',
        'Product Name_1': 'MeterSubCategory',
        'ResourceLocation':'ResourceLocation',
        'CustomerName':'CustomerName',
        'Usage Type': 'UnitType',
        'Usage Quantity': 'Quantity',
        'total': 'PVP Total'
    })




# Realizar transformaciones en los dataframes (ejemplo)
    #df_result1 = pd.concat([df1, df2], ignore_index=True)
    #df_result2 = pd.concat([df1, df2], ignore_index=True)
    df_result = pd.concat([df1, df2], ignore_index=True)
   


# Cambiar las fechas en el campo UsageDate en df_result
    df_result.loc[df_result['UsageDate'] < first_day_of_current_month, 'UsageDate'] = first_day_of_current_month

# Crear un nuevo DataFrame con las columnas deseadas en el orden especificado
    #columns = ["CustomerId", "CustomerName", "UsageDate", "MeterType", "MeterSubCategory", 
    #       "ResourceLocation", "ResourceGroup", "UnitType", "Quantity", "PVP Total"]
    #df_result = df_result.reindex(columns=columns)

# Formatear la columna UsageDate en el formato "día/mes/año"
    df_result['UsageDate'] = pd.to_datetime(df_result['UsageDate']).dt.strftime('%d/%m/%Y')

# Reorganizar las columnas
    columns_order = ["CustomerId", "CustomerName", "UsageDate", "MeterType", "MeterSubCategory", 
                     "ResourceLocation", "ResourceGroup", "UnitType", "Quantity", "PVP Total"]
    df_result = df_result[columns_order]

# Generar un nuevo archivo Excel
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df_result.to_excel(writer, sheet_name='Result', index=False)
    
    # Escribir df2 en una nueva hoja llamada 'DF2' comenzando desde la columna A
    #df2.to_excel(writer, sheet_name='DF2', startrow=0, startcol=0, index=False)

    output.seek(0)

# Guardar el DataFrame resultante en un nuevo archivo Excel
#    output_excel_file = "Resultado.xlsx"
 #   df_result.to_excel(output_excel_file, index=False)

 #   print("Archivo procesado y guardado como Resultado.xlsx")

 #   return render_template("index.html")

# Crear una respuesta para descargar el archivo
    response = make_response(send_file(output, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'))
    response.headers['Content-Disposition'] = 'attachment; filename=Resultado.xlsx'

    # Enviar un mensaje de confirmación
    confirmation_message = 'Los archivos fueron procesados y el resultado se ha descargado correctamente.'
    return jsonify(message=confirmation_message)
    #return response

if __name__ == '__main__':
    app.run(debug=True)
