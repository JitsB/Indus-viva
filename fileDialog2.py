import tkinter as tk
from tkinter import filedialog
# import modin.pandas as pd
import pandas as pd
import os
from datetime import datetime
import xlsxwriter

# import os
# os.environ["MODIN_ENGINE"] = "dask"  # Modin will use Dask
# from distributed import Client
# client = Client(memory_limit='8GB')

#Jahan pe blank hai toh bill to state se lena hai
#Or take from buyer state

# Highlight rows where you have made changes

# if H=AN, cgst, sgst else igst

# To Do: 
# 1. Optimize the code
# 2. Modularize the code
# 3. Make it more readable
# 4. Log time for different data sizes



def calculate_tax(amount, taxPercent):
	answer = amount * (taxPercent/100)
	print("Answer: ",answer)
	return round(answer,2)

def calculate_totalamt(amount, taxAmount):
	answer = amount + taxAmount
	print("Answer totalamt: ",answer)
	return round(answer, 2)

def replicate_data(data):
	# output_data = pd.DataFrame(columns = )
	print("Data shape: ",data.shape)
	for i in range(7):
		data = data.append(data)
	
	print("Data shape after: ",data.shape)
	return data

def check_cgst(billingState, buyerState):
	print("Billing State: ",billingState)
	print("Buyer State: ",buyerState)

	billingState = billingState.lower()
	buyerState = buyerState.lower()

	if billingState == buyerState:
		return "CGST"
	else:
		return "IGST"


# def write_data(input_data, output_data, input_index, output_index):
	

def process_data(data):
	output_data = pd.DataFrame(columns = ["Invoice number","Invoice date","Customer name","Customer GSTIN","Place of Supply","Item Description","Qty","SAC Code","Taxable value","GST Rate","CGST Amount","SGST Amount","IGST Amount","Total Item Value","Total Invoice Value", "Correction if any?"])

	output_index = 1
	index = 0
	
	while index < data.shape[0]:
		print("Index: ",index)
		output_indices = []

		#For the first product
		courierCharge = 0
		transactionCharge = 0

		totalAmount = 0
		firstProductIndex = index

		invoice_no = data.iloc[index]["Invoice No"]
		
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

			taxAmount = calculate_tax(taxableAmount, taxRate)
			output_data.at[output_index, "IGST Amount"] = taxAmount
			output_data.at[output_index, "Total Item Value"] = calculate_totalamt(taxableAmount, taxAmount)

			if data.iloc[index]['Product CGST Amt'] > 0:
				output_data.at[output_index, "Correction if any?"] = "Yes"
			
			else:
				output_data.at[output_index, "Correction if any?"] = "No"

			output_data.at[output_index, "CGST Amount"] = 0
			output_data.at[output_index, "SGST Amount"] = 0
			output_data.at[output_index, "Total Item Value"] = 0
		else:
			csgtRate, sgstRate = taxRate/2, taxRate/2
			taxAmount = calculate_tax(taxableAmount, csgtRate)

			output_data.at[output_index, "IGST Amount"] = 0
			output_data.at[output_index, "Total Item Value"] = 0

			output_data.at[output_index, "CGST Amount"] = taxAmount
			output_data.at[output_index, "SGST Amount"] = taxAmount
			output_data.at[output_index, "Total Item Value"] = calculate_totalamt(taxableAmount, (taxAmount*2)
			) #taxAmount is addition of cgst + sgst

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
		while i < len(data):
			
			if data.iloc[i]["Invoice No"] == invoice_no:
				output_index += 1
				output_indices.append(output_index)

				output_data.at[output_index, "Invoice number"] = data.iloc[i]["Invoice No"]
				output_data.at[output_index, "Invoice date"] = data.iloc[i]["Invoice Date"]
				output_data.at[output_index, "Customer name"] = data.iloc[i]["Distributor Name"]
				output_data.at[output_index, "Customer GSTIN"] = data.iloc[i]["Buyer GST"]
				output_data.at[output_index, "Place of Supply"] = data.iloc[i]["Bill To State"]
				output_data.at[output_index, "SAC Code"] = data.iloc[i]["HSN"]

				output_data.at[output_index, "Item Description"] = data.iloc[i]["Product Name"]
				output_data.at[output_index, "Taxable value"] = data.iloc[i]["Product Ass Val"]
				output_data.at[output_index, "GST Rate"] = data.iloc[i]["Product Tax %"]
				output_data.at[output_index, "Qty"] = data.iloc[i]["Qty"]

				taxableAmount = data.iloc[i]["Product Ass Val"]
				taxRate = data.iloc[i]["Product Tax %"]

				if check_cgst(data.iloc[index]['Billing'], data.iloc[index]['Buyer State']) == 'IGST':
					taxAmount = calculate_tax(taxableAmount, taxRate)
					output_data.at[output_index, "IGST Amount"] = taxAmount
					output_data.at[output_index, "Total Item Value"] = calculate_totalamt(taxableAmount, taxAmount)

					output_data.at[output_index, "CGST Amount"] = 0
					output_data.at[output_index, "SGST Amount"] = 0
					output_data.at[output_index, "Total Item Value"] = 0

					if data.iloc[i]['Product CGST Amt'] > 0:
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
		output_data.at[output_index, "Invoice number"] = data.iloc[index]["Invoice No"]
		output_data.at[output_index, "Invoice date"] = data.iloc[index]["Invoice Date"]
		output_data.at[output_index, "Customer name"] = data.iloc[index]["Distributor Name"]
		output_data.at[output_index, "Customer GSTIN"] = data.iloc[index]["Buyer GST"]
		output_data.at[output_index, "Qty"] = data.iloc[index]["Qty"]

		output_data.at[output_index, "Place of Supply"] = data.iloc[index]["Bill To State"]
		output_data.at[output_index, "Item Description"] = "Courier"
		output_data.at[output_index, "SAC Code"] = SAC_code


		
		output_data.at[output_index, "Taxable value"] = courierCharge

		output_data.at[output_index, "GST Rate"] = data.iloc[index]["Courier Tax %"]
		taxRate = data.iloc[firstProductIndex]["Courier Tax %"]

		if check_cgst(data.iloc[index]['Billing'], data.iloc[index]['Buyer State']) == 'IGST':
			taxAmount = calculate_tax(courierCharge, taxRate)
			output_data.at[output_index, "IGST Amount"] = taxAmount
			output_data.at[output_index, "Total Item Value"] = calculate_totalamt(courierCharge, taxAmount)

			output_data.at[output_index, "CGST Amount"] = 0
			output_data.at[output_index, "SGST Amount"] = 0
			output_data.at[output_index, "Total Item Value"] = 0

			if data.iloc[index]['Courier CGST Amt'] > 0:
				output_data.at[output_index, "Correction if any?"] = "Yes"
			else:
				output_data.at[output_index, "Correction if any?"] = "No"
		else:
			output_data.at[output_index, "IGST Amount"] = 0
			output_data.at[output_index, "Total Item Value"] = 0

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
		output_data.at[output_index, "Invoice number"] = data.iloc[index]["Invoice No"]
		output_data.at[output_index, "Invoice date"] = data.iloc[index]["Invoice Date"]
		output_data.at[output_index, "Customer name"] = data.iloc[index]["Distributor Name"]
		output_data.at[output_index, "Customer GSTIN"] = data.iloc[index]["Buyer GST"]
		output_data.at[output_index, "Qty"] = data.iloc[index]["Qty"]

		output_data.at[output_index, "Place of Supply"] = data.iloc[index]["Bill To State"]
		output_data.at[output_index, "Item Description"] = "Transaction Charges"
		output_data.at[output_index, "SAC Code"] = SAC_code
		
		

		output_data.at[output_index, "Taxable value"] = transactionCharge
		
		output_data.at[output_index, "GST Rate"] = data.iloc[index]["Tax%"]
		taxRate = data.iloc[index]["Tax%"]

		if check_cgst(data.iloc[index]['Billing'], data.iloc[index]['Buyer State']) == 'IGST':
			taxAmount = calculate_tax(transactionCharge, taxRate)
			output_data.at[output_index, "IGST Amount"] = taxAmount
			output_data.at[output_index, "Total Item Value"] = calculate_totalamt(transactionCharge, taxAmount)

			output_data.at[output_index, "CGST Amount"] = 0
			output_data.at[output_index, "SGST Amount"] = 0
			output_data.at[output_index, "Total Item Value"] = 0

			if data.iloc[index]['Transaction CGST Amt'] > 0:
				output_data.at[output_index, "Correction if any?"] = "Yes"
			else:
				output_data.at[output_index, "Correction if any?"] = "No"
		else:

			output_data.at[output_index, "IGST Amount"] = 0
			output_data.at[output_index, "Total Item Value"] = 0
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

if __name__ == "__main__":

	root = tk.Tk()
	root.withdraw()

	print("Innovage Technologies")
	print()
	print()

	print("Opening file dialog, Please Wait")

	file_path = filedialog.askopenfilename()
	

	columns = ['Distributor Name','Invoice No','Invoice Date','Supplier GSTIN','Buyer State','Buyer GST','Product Name','Qty','Invoice Amount','Courier Ass Val','Courier Tax %','Product Ass Val','Product CGST Amt','Product SGST Amt','Product IGST Amt','Product Tax %','Total Amount','Transaction Ass Val','Tax%','HSN','Bill To State']

	data = pd.read_excel(file_path)

	# data = data.head(10)

	print("Transforming excel to desired format, please wait")

	startTime = datetime.now()
	output_data = process_data(data)

	timeToProcess = datetime.now()-startTime
	processTime = datetime.now()

	
	print("File path where the output excel is stored: ",file_path)

	output_csv_file = file_path+"_output.xlsx"


	for i in range(len(output_data)):
		output_data.loc[[i]].to_csv(output_csv_file, index=False, header=False, mode='a')

	# workbook = xlsxwriter.Workbook(file_path+"_output.xlsx", {'constant_memory': True})
	# worksheet = workbook.add_worksheet()

	# for index, row in output_data.iterrows():
	# 	print("index: ",index)
	# 	print("row: ",row)
	# 	worksheet.write_row(row.astype(str))
	
	# workbook.close()

	

	# worksheet.write(output_data)

	# workbook.close()

	# writer = pd.ExcelWriter(file_path+"_output.xlsx", engine='xlsxwriter')

	# # Convert the dataframe to an XlsxWriter Excel object.
	# output_data.to_excel(writer, sheet_name='Sheet1')

	# # # Close the Pandas Excel writer and output the Excel file.
	# writer.save()

	# output_data.to_excel(file_path+"_output.xlsx")

	print("Time taken to write the excel: ")
	timeTaken = datetime.now() - processTime
	print(timeTaken)

	print("Total Time taken: ", (datetime.now() - startTime))

	input("Press any key to exit")

	
#Look at ways of reading data from excel in chunks
#Look at ways of optimizing the processing code
#Optimizing data dumping part