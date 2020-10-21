import pandas as pd

inputDF = pd.read_excel("./data/Copy of BLR Sep-2020.xlsx")

inputDF = inputDF.head(10)

data = inputDF.copy(deep = True)

tempDF = data.groupby(['Invoice No'])['Courier Ass Val','Transaction Ass Val'].sum().reset_index() #Summing courier and transaction values based on invoice no

data = data[['SL No', 'ID', 'Distributor Name', 'Invoice No', 'Invoice Date',
       'Branch', 'Billing', 'Supplier GSTIN', 'Buyer GST', 'Buyer State',
       'Product Name', 'Qty', 'Invoice Amount', 'Courier Charge',
      'Courier Tax %', 'Courier CGST Amt',
       'Courier SGST Amt', 'Courier IGST Amt', 'Product Ass Val',
       'Product CGST Amt', 'Product SGST Amt', 'Product IGST Amt',
       'Product Tax %', 'Total Amount', 'Total Cess', 'Transaction CGST Amt',
       'Transaction SGST Amt', 'Transaction IGST Amt',
       'Transaction Charges', 'Tax%', 'CGST Amt', 'BackDate SGST Amt',
       'BackDate IGST Amt', 'BackDate Ass Val', 'BackDate Charges',
       'BackDate Tax%', 'Total Final Amount', 'Payment Mode', 'Bill To State',
       'HSN']] # Filtering input for relevant columns
      
data = pd.merge(data, tempDF, on='Invoice No') # Merging input data and grouped courier and transaction values

data['Courier Amt'] = round(data['Courier Ass Val'] * (data['Courier Tax %']/100 + 1),2) # Calculating courier and transaction amounts
data['Transaction Amt'] = round(data['Transaction Ass Val'] * (data['Tax%']/100 + 1),2)


data[['Buyer State','Billing']] = data[['Buyer State','Billing']].fillna('')
data[['Courier CGST Amt','Courier SGST Amt','Courier IGST Amt','Courier Tax %']] = data[['Courier CGST Amt','Courier SGST Amt','Courier IGST Amt','Courier Tax %']].fillna(0) #Filling NA values


data['CGST Flag'] = data.apply(lambda x: True if x['Billing'].lower() == x['Buyer State'].lower() else False, axis=1)
data['Corrections if any?'] = data.apply(lambda x: 'No' if x['Billing'].lower() == x['Buyer State'].lower() and x['Product CGST Amt'] > 0 else 'No' if x['Billing'].lower() != x['Buyer State'].lower() and x['Product IGST Amt'] > 0 else 'Yes', axis=1)

data[['Courier CGST Amt','Courier IGST Amt','Buyer State','Billing', 'CGST Flag']].head() #Setting CGST flag based on buyer state and billing values


data['Final Courier CGST Amt'], data['Final Courier SGST Amt'], data['Final Courier IGST Amt'],data['Final Transaction CGST Amt'],data['Final Transaction SGST Amt'],data['Final Transaction IGST Amt'],data['Final Product CGST Amt'],data['Final Product SGST Amt'],data['Final Product IGST Amt']  = [0,0,0,0,0,0,0,0,0] #Initializing courier cgst,sgst,final amount values


data.loc[(data['CGST Flag']==True),['Final Courier CGST Amt']] = (data['Courier Ass Val'] * (data['Courier Tax %']).astype('int') / 200)
data.loc[(data['CGST Flag']==True),['Final Courier SGST Amt']] = (data['Courier Ass Val'] * data['Courier Tax %'] / 200)
data.loc[(data['CGST Flag']==False),['Final Courier IGST Amt']] = (data['Courier Ass Val'] * data['Courier Tax %'] / 100)

data.loc[(data['CGST Flag']==True),['Final Transaction CGST Amt']] = (data['Transaction Ass Val'] * (data['Tax%']).astype('int') / 200)
data.loc[(data['CGST Flag']==True),['Final Transaction SGST Amt']] = (data['Transaction Ass Val'] * data['Tax%'] / 200)
data.loc[(data['CGST Flag']==False),['Final Transaction IGST Amt']] = (data['Transaction Ass Val'] * data['Tax%'] / 100)

data.loc[(data['CGST Flag']==True),['Final Product CGST Amt']] = (data['Product Ass Val'] * (data['Product Tax %']).astype('int') / 200)
data.loc[(data['CGST Flag']==True),['Final Product SGST Amt']] = (data['Product Ass Val'] * data['Product Tax %'] / 200)
data.loc[(data['CGST Flag']==False),['Final Product IGST Amt']] = (data['Product Ass Val'] * data['Product Tax %'] / 100) # Calculating sgst,cgst,igst amounts for courier, transaction and products


data['Total Courier Amt'] = round(data['Courier Ass Val'] + data['Final Courier CGST Amt'] + data['Final Courier SGST Amt'] + data['Final Courier IGST Amt'],2)
data['Total Transaction Amt'] = round(data['Transaction Ass Val'] + data['Final Transaction CGST Amt'] + data['Final Transaction SGST Amt'] + data['Final Transaction IGST Amt'],2)
data['Total Product Amt'] = round(data['Product Ass Val'] + data['Final Product CGST Amt'] + data['Final Product SGST Amt'] + data['Final Product IGST Amt'],2)

newTempDF = data.groupby(['Invoice No'])['Total Product Amt'].sum().reset_index().rename(columns={'Total Product Amt':'Total Product Amt1'})
final_df = pd.merge(data, newTempDF, on='Invoice No')

print("data head: ",final_df.head())

final_df['Total Invoice Value'] = round(final_df['Total Courier Amt'] + final_df['Total Transaction Amt'] + final_df['Total Product Amt1'],2)
final_df = final_df.head(100)

final_df = final_df[['Distributor Name', 'Invoice No', 'Invoice Date',
       'Billing', 'Supplier GSTIN','Buyer State','Buyer GST',
       'Product Name', 'Qty','Courier Tax %', 'Product Ass Val', 'Product Tax %', 'Total Amount','Transaction Charges', 'Tax%', 'CGST Amt',
       'Total Final Amount','Bill To State', 'HSN', 'Courier Ass Val',
       'Transaction Ass Val', 'Courier Amt', 'Transaction Amt', 'CGST Flag',
       'Final Courier CGST Amt', 'Final Courier SGST Amt',
       'Final Courier IGST Amt', 'Final Transaction CGST Amt',
       'Final Transaction SGST Amt', 'Final Transaction IGST Amt',
       'Final Product CGST Amt', 'Final Product SGST Amt',
       'Final Product IGST Amt', 'Total Courier Amt', 'Total Transaction Amt',
       'Total Product Amt', 'Total Invoice Value', 'Total Product Amt1', 'Corrections if any?']] #Filtering values


#Splitting courier, transaction and subsequent product values for a line item
df_1 = final_df[['Invoice No','Courier Ass Val','Transaction Ass Val']].melt(id_vars=["Invoice No"], var_name='Product Name',value_name="Taxable Value").drop_duplicates(keep='first')

df_2 = final_df[['Invoice No','Courier Tax %','Tax%']].melt(id_vars=["Invoice No"],var_name='Product Name',value_name="GST Rate").drop_duplicates(keep='first')

df_3 = final_df[['Invoice No','Final Courier CGST Amt','Final Transaction CGST Amt']].melt(id_vars=["Invoice No"],var_name='Product Name',value_name="CGST Amount").drop_duplicates(keep='first')

df_4 = final_df[['Invoice No','Final Courier IGST Amt','Final Transaction IGST Amt']].melt(id_vars=["Invoice No"], var_name='Product Name',value_name="IGST Amount").drop_duplicates(keep='first')

df_5 = final_df[['Invoice No','Total Courier Amt','Total Transaction Amt']].melt(id_vars=["Invoice No"],var_name='Product Name', value_name="Total Item Value").drop_duplicates(keep='first')

df_5[df_5['Invoice No'] == 'IV/GST/20-21/50126']

filtered_input_df = final_df[['Distributor Name', 'Invoice Date',
       'Buyer GST','Buyer State', 'HSN',
       'Product Name', 'Qty','Invoice No','Product Ass Val', 'Product Tax %','Final Product CGST Amt',
       'Final Product IGST Amt','Total Product Amt','Total Invoice Value', 'Corrections if any?']] #Filtering columns

print("data head: ",filtered_input_df.head())

filtered_input_df = filtered_input_df.rename(columns={'HSN':'SAC Code','Product Ass Val': "Taxable Value", "Product Tax %":"GST Rate","Final Product CGST Amt":"CGST Amount",'Final Product IGST Amt':"IGST Amount",'Total Product Amt':"Total Item Value"})

temp = pd.concat([df_1, df_2[['GST Rate']], df_3[['CGST Amount']], df_4[['IGST Amount']], df_5[['Total Item Value']]], axis=1) #Concatenating courier, transaction and subsequent products in a single df

new_final_output = pd.concat([filtered_input_df, temp], axis=0, ignore_index=True)

new_final_output[new_final_output['Invoice No']=='IV/GST/20-21/51843']

product_names = {'Courier Ass Val':'Courier','Total Courier Amt':'Courier','Courier Tax %':'Courier','Final Courier CGST Amt':'Courier','Final Courier IGST Amt':'Courier','Total Courier Amt':'Courier','Tax %':'Transaction Charges','Total Transaction Amt':'Transaction Charges','Transaction Ass Val':'Transaction Charges','Final Transaction IGST Amt':'Transaction Charges','Final Transaction CGST Amt':'Transaction Charges'}

new_final_output['Product Name'] = new_final_output['Product Name'].map(product_names).fillna(new_final_output['Product Name'])

idx = 13
new_final_output.insert(loc=idx, column='SGST Amount', value=new_final_output['CGST Amount'].values)


new_final_output = new_final_output.rename(columns={'Invoice No':'Invoice Number','Product Name':'Item Description','Distributor Name':'Customer Name','Buyer GST':'Customer GSTIN','Buyer State':'Place of Supply'})

new_final_output = new_final_output.sort_values(by=['Invoice Number','Taxable Value'],ascending=(True, False))

new_final_output = new_final_output.fillna(method='ffill')


# new_final_output.to_excel("./data/Copy of BLR Sep-2020_test.xlsx")