import sys
import os
import collections


outputDir = r'C:\Users\sthomas\Documents\Printnet Workflows\AST\40058 Micro Focus DRS and Check\scripts'

drs_data = os.path.join(outputDir, "Microfocus31808.DRSAWExch.DocSol.dat")
chk_data = os.path.join(outputDir, "Microfocus31808.EXCHNEW.DocSol.chk")


chk_only_file = os.path.join(outputDir, "chk_only_file.txt")
drs_only_file = os.path.join(outputDir, "drs_only_file.txt")

single_chk_file = os.path.join(outputDir, "single_chk_file.txt")
single_drs_file = os.path.join(outputDir, "single_drs_file.txt")

two_chk_file = os.path.join(outputDir, "two_chk_file.txt")
two_drs_file = os.path.join(outputDir, "two_drs_file.txt")
two_chk_only_file = os.path.join(outputDir, "two_chk_only_file.txt")

three_chk_file = os.path.join(outputDir, "three_chk_file.txt")
three_drs_file = os.path.join(outputDir, "three_drs_file.txt")
three_chk_only_file = os.path.join(outputDir, "three_chk_only_file.txt")


single_chk_count = 0
single_drs_count = 0

two_chk_count = 0
two_drs_count = 0
two_chk_only_count = 0

three_chk_count = 0
three_drs_count = 0
three_chk_only_count = 0

drs_only_count = 0
chk_only_count = 0

total_count = 0

with open(drs_data, 'rb') as drs:
    with open(chk_data, 'rb') as chk:
    
        with open(chk_only_file, 'wb') as chk_only:
            with open(drs_only_file, 'wb') as drs_only:
            
                with open(single_chk_file, 'wb') as single_chk:
                    with open(single_drs_file, 'wb') as single_drs:
                    
                        with open(two_chk_file, 'wb') as two_chk:
                            with open(two_drs_file, 'wb') as two_drs:
                                with open(two_chk_only_file, 'wb') as two_chk_only:
                                
                                    with open(three_chk_file, 'wb') as three_chk:
                                        with open(three_drs_file, 'wb') as three_drs:
                                            with open(three_chk_only_file, 'wb') as three_chk_only:
                                                                        
                                                # Create a dict of the DRS data 
                                                drs_dict = collections.defaultdict(str)
                                                for line in drs:
                                                    if line[0] == "H":
                                                        drs_comp_num = line[343:348].strip()
                                                        drs_acct_num = line[397:407].strip()
                                                        drs_comp_acct = drs_comp_num + drs_acct_num
                                                        drs_dict[drs_comp_acct] = line
                                                
                                                # Create a dict of the CHECK data 
                                                chk_dict = collections.defaultdict(list)
                                                for row in chk:
                                                    chk_comp_acct = row[1:16]
                                                    chk_dict[chk_comp_acct].append(row)

                                                # Check if check account in DRS data:
                                                for cmp_act in sorted(chk_dict.keys()):
                                                    # print cmp_act
                                                    if drs_dict.get(cmp_act):

                                                        # print "MATCH"
                                                        if len(chk_dict[cmp_act]) == 3:
                                                            '''
                                                            Multiple checks - 
                                                            - write DRS record to drs multi file
                                                            - write first chk data to chk multi file
                                                            - write other chks to chk multi only file
                                                            '''
                                                            three_drs.write(drs_dict[cmp_act])
                                                            three_drs_count += 1
                                                            
                                                            three_chk.write(chk_dict[cmp_act][0])
                                                            three_chk_count += 1
                                                            
                                                            for chk_rec in chk_dict[cmp_act][1:]:
                                                                three_chk_only.write(chk_rec)
                                                                three_chk_only_count += 1
                                                            del drs_dict[cmp_act]
                                                            del chk_dict[cmp_act]
                                                                
                                                        elif len(chk_dict[cmp_act]) == 2:
                                                            two_drs.write(drs_dict[cmp_act])
                                                            two_drs_count += 1
                                                            
                                                            two_chk.write(chk_dict[cmp_act][0])
                                                            two_chk_count += 1
                                                            
                                                            for chk_rec in chk_dict[cmp_act][1:]:
                                                                two_chk_only.write(chk_rec)
                                                                two_chk_only_count += 1
                                                            del drs_dict[cmp_act]
                                                            del chk_dict[cmp_act]
                                                        
                                                        elif len(chk_dict[cmp_act]) == 1:
                                                            # print "single"
                                                            ''' 
                                                            Single checks -
                                                            - write DRS record to drs file
                                                            - write check record to chk file
                                                            '''
                                                            single_drs.write(drs_dict[cmp_act])
                                                            single_drs_count += 1
                                                            
                                                            single_chk.write(chk_dict[cmp_act][0])
                                                            single_chk_count += 1
                                                            
                                                            del chk_dict[cmp_act]
                                                            del drs_dict[cmp_act]
                                                        # Delete the records from the dicts as processed. 
                                                    
                                                    else:
                                                        for chk_rec in chk_dict[cmp_act]:
                                                            '''
                                                            No match:
                                                            - write check records to chk only file
                                                            Delete each record from the dicts as processed. 
                                                            '''
                                                            chk_only.write(chk_rec)
                                                            chk_only_count += 1
                                                            
                                                        del chk_dict[cmp_act]
                                                        
                                                # print len(drs_dict.keys())
                                                
                                                '''
                                                Write the unmatched DRS data to the DRS only file
                                                '''
                                                print len(drs_dict.keys())
                                                for acct_no in sorted(drs_dict.keys()):       
                                                    drs_only.write(drs_dict[acct_no])
                                                    drs_only_count += 1
                                                
                                                total_count = sum([single_drs_count,single_chk_count,
                                                    two_chk_count, two_drs_count, two_chk_only_count,
                                                    three_chk_count, three_drs_count, three_chk_only_count,
                                                    drs_only_count, chk_only_count])

                                                
                                                print "Single DRS and Check: {}".format(single_drs_count)
                                                print "Two DRS and Check: {} - {}".format(two_drs_count, two_chk_count+two_chk_only_count)
                                                print "Three DRS and Check: {} - {}".format(three_drs_count, three_chk_count+three_chk_only_count)
                                                print "DRS only: {}".format(drs_only_count)
                                                print "Check only: {}".format(chk_only_count)
                                                print "Total counts: {}".format(total_count)
                                        
                                
                                    
