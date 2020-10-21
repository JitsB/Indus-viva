import tkinter as tk
from tkinter import filedialog
# import modin.pandas as pd
import pandas as pd
import os
from datetime import datetime
import xlsxwriter
import csv
import numpy as np
import multiprocessing
import time
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# import os
# os.environ["MODIN_ENGINE"] = "dask"  # Modin will use Dask
# from distributed import Client
# client = Client(memory_limit='8GB')

#Jahan pe blank hai toh bill to state se lena hai
#Or take from buyer state

# Highlight rows where you have made changes

# if H=AN, cgst, sgst else igst

def calculate_tax(amount, taxPercent):
	
	return round(amount * (taxPercent/100),2)

def calculate_totalamt(amount, taxAmount):
	
	return round(amount + taxAmount, 2)

def check_cgst(billingState, buyerState):
	billingState = billingState.lower()
	buyerState = buyerState.lower()
	if billingState == buyerState:
		return "CGST"
	else:
		return "IGST"

def process_data_2(inputDF):
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

	final_df['Total Invoice Value'] = round(final_df['Total Courier Amt'] + final_df['Total Transaction Amt'] + final_df['Total Product Amt1'],2)
	

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

	return new_final_output


def process_data(data):
	output_data = pd.DataFrame(columns = ['Sr.No',"Invoice number","Invoice date","Customer name","Customer GSTIN","Place of Supply","Item Description","Qty","SAC Code","Taxable value","GST Rate","CGST Amount","SGST Amount","IGST Amount","Total Item Value","Total Invoice Value", "Correction if any?"])

	output_index = 1
	index = 0

	# while index < 14229:c
	while index < (data.shape[0]-1):

		print("Processing item no: ",(index))
		
		output_indices = []

		#For the first product
		courierCharge = 0
		transactionCharge = 0

		totalAmount = 0
		firstProductIndex = index

		invoice_no = data.iloc[index]["Invoice No"]
		output_data.at[output_index, 'Sr.No'] = output_index
		
		output_data.at[output_index, "Invoice number"] = data.iloc[index]["Invoice No"]
		output_data.at[output_index, "Invoice date"] = data.iloc[index]["Invoice Date"]
		output_data.at[output_index, "Customer name"] = data.iloc[index]["Distributor Name"]
		output_data.at[output_index, "Customer GSTIN"] = data.iloc[index]["Buyer GST"]
		output_data.at[output_index, "Place of Supply"] = data.iloc[index]["Buyer State"]
		output_data.at[output_index, "Item Description"] = data.iloc[index]["Product Name"]
		output_data.at[output_index, "SAC Code"] = data.iloc[index]["HSN"]
		output_data.at[output_index, "Qty"] = data.iloc[index]["Qty"]

		SAC_code = data.iloc[index]["HSN"]
		
		output_data.at[output_index, "Taxable value"] = data.iloc[index]["Product Ass Val"]
		output_data.at[output_index, "GST Rate"] = data.iloc[index]["Product Tax %"]

		taxableAmount = data.iloc[index]["Product Ass Val"]
		taxRate = data.iloc[index]["Product Tax %"]

		if check_cgst(data.iloc[index]['Billing'], data.iloc[index]['Buyer State']) == 'IGST':
			output_data.at[output_index, "CGST Amount"] = 0
			output_data.at[output_index, "SGST Amount"] = 0

			taxAmount = calculate_tax(taxableAmount, taxRate)
			output_data.at[output_index, "IGST Amount"] = taxAmount
			output_data.at[output_index, "Total Item Value"] = calculate_totalamt(taxableAmount, taxAmount)

			if data.iloc[index]['Product CGST Amt'] > 0:
				output_data.at[output_index, "Correction if any?"] = "Yes"
			
			else:
				output_data.at[output_index, "Correction if any?"] = "No"

			
			
		else:
			output_data.at[output_index, "IGST Amount"] = 0
			output_data.at[output_index, "Total Item Value"] = 0

			csgtRate, sgstRate = taxRate/2, taxRate/2
			taxAmount = calculate_tax(taxableAmount, csgtRate)

			output_data.at[output_index, "CGST Amount"] = taxAmount
			output_data.at[output_index, "SGST Amount"] = taxAmount
			
			taxAmount *= 2
			output_data.at[output_index, "Total Item Value"] = calculate_totalamt(taxableAmount, (taxAmount)) #taxAmount is addition of cgst + sgst

			if data.iloc[index]['Product IGST Amt'] > 0:
				output_data.at[output_index, "Correction if any?"] = "Yes"
			else:
				output_data.at[output_index, "Correction if any?"] = "No"
		
		totalAmount += output_data.at[output_index, "Total Item Value"]
		
		courierCharge += data.iloc[index]["Courier Ass Val"]
		transactionCharge += data.iloc[index]["Transaction Ass Val"]

		output_indices.append(output_index)

		i = index + 1
		#### Subsequent products #
		# for i in range(index+1,len(data)): #For subsequent products
		while i < data.shape[0]:
			if data.iloc[i]["Invoice No"] == invoice_no:
				output_index += 1
				output_indices.append(output_index)

				output_data.at[output_index, 'Sr.No'] = output_index
				output_data.at[output_index, "Invoice number"] = data.iloc[i]["Invoice No"]
				output_data.at[output_index, "Invoice date"] = data.iloc[i]["Invoice Date"]
				output_data.at[output_index, "Customer name"] = data.iloc[i]["Distributor Name"]
				output_data.at[output_index, "Customer GSTIN"] = data.iloc[i]["Buyer GST"]
				output_data.at[output_index, "Place of Supply"] = data.iloc[index]["Buyer State"]
				output_data.at[output_index, "SAC Code"] = data.iloc[i]["HSN"]

				output_data.at[output_index, "Item Description"] = data.iloc[i]["Product Name"]
				output_data.at[output_index, "Taxable value"] = data.iloc[i]["Product Ass Val"]
				output_data.at[output_index, "GST Rate"] = data.iloc[i]["Product Tax %"]
				output_data.at[output_index, "Qty"] = data.iloc[i]["Qty"]

				taxableAmount = data.iloc[i]["Product Ass Val"]
				taxRate = data.iloc[i]["Product Tax %"]

				if check_cgst(data.iloc[index]['Billing'], data.iloc[index]['Buyer State']) == 'IGST':
					output_data.at[output_index, "CGST Amount"] = 0
					output_data.at[output_index, "SGST Amount"] = 0

					taxAmount = calculate_tax(taxableAmount, taxRate)
					output_data.at[output_index, "IGST Amount"] = taxAmount
					output_data.at[output_index, "Total Item Value"] = calculate_totalamt(taxableAmount, taxAmount)

					if data.iloc[i]['Product CGST Amt'] > 0:
						output_data.at[output_index, "Correction if any?"] = "Yes"
					else:
						output_data.at[output_index, "Correction if any?"] = "No"

				else:

					output_data.at[output_index, "IGST Amount"] = 0

					csgtRate, sgstRate = taxRate/2, taxRate/2
					taxAmount = calculate_tax(taxableAmount, csgtRate)
					output_data.at[output_index, "CGST Amount"] = taxAmount
					output_data.at[output_index, "SGST Amount"] = taxAmount

					output_data.at[output_index, "Total Item Value"] = calculate_totalamt(taxableAmount, (taxAmount*2)) #taxAmount is addition of cgst + sgst

					if data.iloc[i]['Product IGST Amt'] > 0:
						output_data.at[output_index, "Correction if any?"] = "Yes"
					else:
						output_data.at[output_index, "Correction if any?"] = "No"

				courierCharge += data.iloc[i]["Courier Ass Val"]
				transactionCharge += data.iloc[i]["Transaction Ass Val"]

				totalAmount += output_data.at[output_index, "Total Item Value"]
				i += 1

			else:
				break
		
		output_index += 1
		output_indices.append(output_index)

		######For courier
		output_data.at[output_index, 'Sr.No'] = output_index
		output_data.at[output_index, "Invoice number"] = data.iloc[index]["Invoice No"]
		output_data.at[output_index, "Invoice date"] = data.iloc[index]["Invoice Date"]
		output_data.at[output_index, "Customer name"] = data.iloc[index]["Distributor Name"]
		output_data.at[output_index, "Customer GSTIN"] = data.iloc[index]["Buyer GST"]
		output_data.at[output_index, "Qty"] = data.iloc[index]["Qty"]

		output_data.at[output_index, "Place of Supply"] = data.iloc[index]["Buyer State"]
		output_data.at[output_index, "Item Description"] = "Courier"
		output_data.at[output_index, "SAC Code"] = SAC_code

		output_data.at[output_index, "Taxable value"] = courierCharge

		output_data.at[output_index, "GST Rate"] = data.iloc[index]["Courier Tax %"]
		taxRate = data.iloc[firstProductIndex]["Courier Tax %"]

		if check_cgst(data.iloc[index]['Billing'], data.iloc[index]['Buyer State']) == 'IGST':
			output_data.at[output_index, "CGST Amount"] = 0
			output_data.at[output_index, "SGST Amount"] = 0

			taxAmount = calculate_tax(courierCharge, taxRate)
			output_data.at[output_index, "IGST Amount"] = taxAmount
			output_data.at[output_index, "Total Item Value"] = calculate_totalamt(courierCharge, taxAmount)

			if data.iloc[index]['Courier CGST Amt'] > 0:
				output_data.at[output_index, "Correction if any?"] = "Yes"
			else:
				output_data.at[output_index, "Correction if any?"] = "No"
		else:
			output_data.at[output_index, "IGST Amount"] = 0
			
			csgtRate, sgstRate = taxRate/2, taxRate/2
			taxAmount = calculate_tax(courierCharge, csgtRate)
			output_data.at[output_index, "CGST Amount"] = taxAmount
			output_data.at[output_index, "SGST Amount"] = taxAmount
			output_data.at[output_index, "Total Item Value"] = calculate_totalamt(courierCharge, (taxAmount*2))

			if data.iloc[index]['Courier IGST Amt'] > 0:
				output_data.at[output_index, "Correction if any?"] = "Yes"
			else:
				output_data.at[output_index, "Correction if any?"] = "No"

		
		totalAmount += output_data.at[output_index, "Total Item Value"]

		
		output_index += 1
		output_indices.append(output_index)
		
		##### For transaction
		output_data.at[output_index, 'Sr.No'] = output_index
		output_data.at[output_index, "Invoice number"] = data.iloc[index]["Invoice No"]
		output_data.at[output_index, "Invoice date"] = data.iloc[index]["Invoice Date"]
		output_data.at[output_index, "Customer name"] = data.iloc[index]["Distributor Name"]
		output_data.at[output_index, "Customer GSTIN"] = data.iloc[index]["Buyer GST"]
		output_data.at[output_index, "Qty"] = data.iloc[index]["Qty"]

		output_data.at[output_index, "Place of Supply"] = data.iloc[index]["Buyer State"]
		output_data.at[output_index, "Item Description"] = "Transaction Charges"
		output_data.at[output_index, "SAC Code"] = SAC_code
		
		output_data.at[output_index, "Taxable value"] = transactionCharge
		
		output_data.at[output_index, "GST Rate"] = data.iloc[index]["Tax%"]
		taxRate = data.iloc[index]["Tax%"]

		if check_cgst(data.iloc[index]['Billing'], data.iloc[index]['Buyer State']) == 'IGST':
			output_data.at[output_index, "CGST Amount"] = 0
			output_data.at[output_index, "SGST Amount"] = 0

			taxAmount = calculate_tax(transactionCharge, taxRate)
			output_data.at[output_index, "IGST Amount"] = taxAmount
			output_data.at[output_index, "Total Item Value"] = calculate_totalamt(transactionCharge, taxAmount)

			if data.iloc[index]['Transaction CGST Amt'] > 0:
				output_data.at[output_index, "Correction if any?"] = "Yes"
			else:
				output_data.at[output_index, "Correction if any?"] = "No"
		else:

			output_data.at[output_index, "IGST Amount"] = 0
			
			csgtRate, sgstRate = taxRate/2, taxRate/2
			taxAmount = calculate_tax(transactionCharge, csgtRate)
			output_data.at[output_index, "CGST Amount"] = taxAmount
			output_data.at[output_index, "SGST Amount"] = taxAmount
			output_data.at[output_index, "Total Item Value"] = calculate_totalamt(transactionCharge, (taxAmount*2))

			if data.iloc[index]['Transaction IGST Amt'] > 0:
				output_data.at[output_index, "Correction if any?"] = "Yes"
			else:
				output_data.at[output_index, "Correction if any?"] = "No"

			
		totalAmount += output_data.at[output_index, "Total Item Value"]
		



		for k in range(len(output_indices)):
			output_data.at[output_indices[k],'Total Invoice Value'] = totalAmount

		output_index += 1
		index = i

	return output_data


def write_output(file_path, *data_to_write):
	print("Inside write_output")

	print("Input data: ")
	print(data_to_write)

	data = pd.DataFrame(data_to_write[0])

	print("Type of input data: ")
	print(type(data))

	columns = ['Sr.No',"Invoice number","Invoice date","Customer name","Customer GSTIN","Place of Supply","Item Description","Qty","SAC Code","Taxable value","GST Rate","CGST Amount","SGST Amount","IGST Amount","Total Item Value","Total Invoice Value", "Correction if any?"]

	with open(file_path+"_output.csv", 'a') as f:
		writer = csv.writer(f)
		for index, row in data.iterrows():
		# for index in range(len(data_to_write)):
			writer.writerow(row)
	

if __name__ == "__main__":

	root = tk.Tk()
	root.withdraw()

	print()
	print()
	print("Innovage Technologies")
	print()
	print()

	print("Opening file dialog, Please Wait")

	file_path = filedialog.askopenfilename()


	columns = ['Sr.No',"Invoice number","Invoice date","Customer name","Customer GSTIN","Place of Supply","Item Description","Qty","SAC Code","Taxable value","GST Rate","CGST Amount","SGST Amount","IGST Amount","Total Item Value","Total Invoice Value", "Correction if any?"]

	data = pd.read_excel(file_path)
	data = data.reset_index()
	

	print("Transforming excel to desired format, please wait")

	startTime = datetime.now()
	output_data = process_data_2(data)

	timeToProcess = datetime.now()-startTime
	processTime = datetime.now()

	path = os.path.dirname(file_path)


	print("File path where the output excel is stored: ",path+"/")

	


	filename = os.path.splitext(file_path)[0].split('/')[-1]
	


	output_data.to_excel(path+"/"+filename+"_output.xlsx")

	print("Output file generated, please check in the path")

	
	input("Press any key to exit")

	
