#!/usr/bin/env python

import csv

import sys

import os

import re

import os.path

import smtplib



#Remove old files

print("Deleting files: jobs.csv & schedulers_info_only.csv\n")

if os.path.isfile("jobs.csv"):

    os.remove("jobs.csv")

if os.path.isfile("schedulers_info_only.csv"):

    os.remove("schedulers_info_only.csv")



#Take the input file from the user read it and appened information in CSV format to schedulers_info_only.csv

input = sys.argv[1]

ofile  = open('schedulers_info_only.csv', "wb+")

writer = csv.writer(ofile, delimiter=';', lineterminator='\n', quoting=csv.QUOTE_NONE, escapechar=' ')



def find_object(start, end):

	object = line[start:end]

	mystring = str(repr(object)).strip("'")

	writer.writerow([mystring])



def insert_string(string):

	simple_string = str(string).strip("'")

	writer.writerow([simple_string])



jobs_line = 0



########### JOBSTREAM INFORMATION #############

print("Gathering Jobstream Information\n")

with open(input, "rb") as ifile:

	for (i, line) in enumerate(ifile):

		if ":" in line:

			break

		elif jobs_line == 0:

			if "SCHEDULE" in line:

				find_object(8, line.find("#"))

				find_object(line.find("#") + 1, line.find(" ", line.find("#")))



			if "VALIDFROM" in line:

				find_object(line.find(" ", line.find("VALIDFROM")),line.find(" ", line.find("/")))



			if "TIMEZONE" in line:

				find_object(line.find("US", line.find("TIMEZONE")) + 3,line.find("US", line.find("TIMEZONE")) + 4)



			if "DESCRIPTION" in line:

				find_object(line.find('"') + 1, len(line) - 2)



			if "DRAFT" in line:

				insert_string("y")



			if "RUNCYCLE" in line:

				if "FREQ=DAILY" in line:

					insert_string("Daily")

				if "FREQ" not in line and "/" not in line:

					insert_string("Calendar")

					find_object(line.find(" ", 13), len(line) - 1)

					find_object(line.find(" ", 4), line.find(" ", 13))

				elif "FREQ" not in line and "/" in line:

					insert_string("Simple")

					find_object(line.find(" ", 13), len(line) - 1)

					find_object(line.find(" ", 4), line.find(" ", 13))

				if "FREQ=WEEKLY" in line:

					insert_string("Weekly")

					find_object(line.find("BYDAY=") + 6, len(line) - 2,)

					find_object(line.find(" ", 4), line.find(" ", 13))



#Close files

ofile.close()

ifile.close()



########## JOBS ###############

ofile  = open('jobs.csv', "wb+")

writer = csv.writer(ofile, delimiter=';', lineterminator='\n', quoting=csv.QUOTE_NONE, escapechar=' ')



jobs_line = 0

filewatch_search = ""

is_filewatch = False

is_inserted = False

sap_job = False

broker_job = False



def set_string(start, end):

    object = line[start:end]

    return str(object).strip("'")

	

def set_string_filewatch(start, end):

    object = lines[start:end]

    return str(repr(object)).strip("'")

	

def set_string_sap(start, end):

	object = sap_line[start:end]

	object = re.sub('\\n', '', object)

	return str(repr(object)).strip("'").strip("\n")



def set_time(start, end):

	return str(line[start:end]) + '|'



def insert_job():

	if type != "":

		rows = type + "," + job_server + "," + job_name + "," + job_description + "," + user_id + "," + submit_type + "," + script_location + "," + script_name + "," + start_time + "," + latest_time + "," + latest_action + "," + deadline + "," + repeating + ",,"  + pred_jobstream + "," + pred_job + "," + pred_type + "," + pred_resolution + "," + file_location + "," + file_name + "," + file_server + "," + file_post_action + "," + sap_user + "," + sap_type + "," + sap_variant + "," + sap_program + "," + return_code + "," + priority + "," + resource

		writer.writerow([rows])

		

def create_opens():

	global file_location

	global file_name

	global file_server

	global file_post_action

	file_server = set_string(6, line.find("#"))

	file_location = set_string(line.find("/"), line.rfind("/") + 1)

	file_name = set_string(line.rfind("/") + 1, line.rfind('"'))

	file_post_action = "Deleted"



def empty_job_information():

	global type

	global job_server

	global job_name

	global job_description

	global user_id

	global submit_type

	global script_location

	global script_name

	global start_time

	global latest_time

	global latest_action

	global deadline

	global repeating

	global pred_jobstream

	global pred_job

	global pred_type

	global pred_resolution

	global file_location

	global file_name

	global file_server

	global file_post_action

	global sap_user

	global sap_type

	global sap_variant

	global sap_program

	global return_code

	global priority

	global resource

	type = ""

	job_server = ""

	job_name = ""

	job_description = ""

	user_id = ""

	submit_type = ""

	script_location = ""

	script_name = ""

	start_time = ""

	latest_time = ""

	latest_action = ""

	deadline = ""

	repeating = ""

	pred_jobstream = ""

	pred_job = ""

	pred_type = ""

	pred_resolution = ""

	file_location = ""

	file_name = ""

	file_server = ""

	file_post_action = ""

	sap_user = ""

	sap_type = ""

	sap_variant = ""

	sap_program = ""

	return_code = ""

	priority = ""

	resource = ""



empty_job_information()

print("Gathering Job Information\n")

with open(input, "rb") as jfile:

	for (j, line) in enumerate(jfile):

		if ":" in line:

			jobs_line = 1

		if jobs_line == 1:

			if "END" in line:

				insert_job()

			if sap_job:

				sap_line = sap_line + line

				sap_job = False

				

				program_index = sap_line.find("program=") + 8

				variant_index = sap_line.find("-v1") + 4

				user_index = sap_line.find("-user") + 6

				

				sap_program = set_string_sap(program_index, sap_line.find(" ", program_index))

				sap_variant = set_string_sap(variant_index, sap_line.find(" ", variant_index))

				sap_user = set_string_sap(user_index, sap_line.find(" ", user_index))

				sap_line = ""

				

			#This will let the process know to not added a filewatch as a job

			if "#" in line and "FW_" in line:

				insert_job()

				is_filewatch = True

				

			if "#" in line and line[0] != " " and line.find("FW_") == -1 and line.find("OPENS") == -1:

				#Do not added filewatch as job, since it is handled differently below

				if is_filewatch:

					empty_job_information()

				if is_inserted == False:

					insert_job()

				empty_job_information()

				is_filewatch = False

				is_inserted = False

				sap_job = False

				broker_job = False

				file_depends_count = 0

				pred_count = 0

				job_server = set_string(0, line.find("#"))

				if job_server == "MDM-TDWB":

					broker_job = True

				job_array = line.split(' ')

				if "AS" in job_array:

					job_array = ''.join(job_array)

					job_array = job_name[len(job_server) + 1:]

					print "Aliased job: " + job_name

				job_name = set_string(line.find("#") + 1, len(line) - 1)

			if "TASKTYPE" in line:

				if broker_job == False:

					type = set_string(line.find("TASKTYPE") + 9, len(line) - 1)

					if type == "WINDOWS":

						windows_job = True

			if "DESCRIPTION" in line:

				job_description = set_string(line.find("DESCRIPTION") + 13, len(line) - 2)

			if "STREAMLOGON" in line:

				user_id = set_string(line.find(" ", 2) + 1, len(line) - 1)

			if "SCRIPTNAME" in line:

				submit_type = "script"

				#Checking for Windows or Unix job

				if line.find("/") != -1:

					script_location = set_string(13, line.rfind("/") + 1)

					script_name = set_string(line.rfind("/") + 1, len(line) - 2)

				else:

					script_location = set_string(13, line.rfind("\\") + 1)

					script_name = set_string(line.rfind("\\") + 1, len(line) - 2)

			if ("AT" in line and line.find("AT") == 1):

				start_time = set_time(line.find("AT") + 3, 8)

			if "UNTIL" in line:

				latest_time = set_time(line.find("UNTIL") + 6, line.find("UNTIL") + 10)

				latest_action = "Suppress"

				if "CANC" in line:

					latest_action = "Cancel"

				if "CONT" in line:

					latest_action = "Continue"

			if "DEADLINE" in line:

				deadline = set_time(line.find("DEADLINE") + 9, line.find("DEADLINE") + 13)

			if "EVERY" in line:

				repeating = set_string(7, 11)

			if "RCCONDSUCC" in line:

				return_code = set_string(line.find('"') + 1, line.rfind('"'))

			if "NEEDS" in line:

				resource = set_string(8, len(line) - 1)

			

			#PRED Section

			if ("FOLLOWS" in line and line.find("FW_") == -1):

				pred_count += 1

				if pred_count > 1:

					insert_job()

					empty_job_information()

				pred_type = "Internal"

				pred_job = set_string(9, len(line) -1)

				pred_resolution = "Same Day"

				if "." in line:

					pred_type = "External"

					pred_jobstream = set_string(9, line.find("."))

					pred_job = set_string(line.find(".") + 1, len(line) -1)

				if pred_job == "@":

					pred_job = ""

				if "PRIOR" in line:

					pred_resolution = "Prior Day"

				if pred_count > 1:

					type = "Pred"

					insert_job()

					empty_job_information()

					is_inserted = True

			

			#File Dependency Section			

			if ("OPENS") in line:

				file_depends_count += 1

				if file_depends_count == 1:

					create_opens()

				

				if file_depends_count > 1:

					insert_job()

					empty_job_information()

					type = "File"

					create_opens()

					insert_job()



			#This will convert filewatch information suitable for the new spreadsheet

			if "FOLLOWS FW_" in line:

				filewatch_search = "#" + set_string(9, len(line) -1)

				filewatch_found = None

				multiple_lines_test = None

				test_break = None

				with open(input, "rb") as search:

					for (k, lines) in enumerate(search):

						if (multiple_lines_test and lines[0] != " "):

							file_name = file_name + set_string_filewatch(0, len(lines) - 2)

							if file_depends_count > 1:

								insert_job()

								is_inserted = True

							break

						elif test_break:

							if file_depends_count > 1:

								insert_job()

								is_inserted = True

							break

						if filewatch_found:

							file_location = set_string_filewatch(lines.find("-file") + 5, lines.rfind("/") + 1)

							file_name = set_string_filewatch(lines.rfind("/") + 1, len(lines) - 2)

							multiple_lines_test = True

							if lines[len(lines) - 1] == '"':

								file_name = set_string_filewatch(lines.rfind("/") + 1, len(lines) - 1)

								multiple_lines_test = False

							test_break = True

						if filewatch_search in lines:

							file_depends_count += 1

							filewatch_found = True

							if file_depends_count > 1:

								insert_job()

								empty_job_information()

								type = "File"

							file_server = set_string_filewatch(0, lines.find("#"))

							file_post_action = "No Action"

							

			#SAP section

			if "DOCOMMAND" in line:

				if broker_job == False:

					sap_line = line

					sap_job = True

					sap_type = "Standard R/3 Job"

					script_location = ""

					script_name = ""

				else:

					type = "Broker"

					submit_type = "Command"

					script_name = job_name

								

ofile.close()



#Send Email to user

os.system("cat x | mailx -a jobs.csv -a schedulers_info_only.csv -s 'REPORT TWS TO JRS OUTPUT' $USER@allstate.com")



print("Completed\n")

print("The Excel files have been emailed to you.\n")


