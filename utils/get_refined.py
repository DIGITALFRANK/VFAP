import boto3
s3 = boto3.client('s3')
sr3 = boto3.resource('s3')


class filename:
    def __init__(self):
        pass

    @staticmethod
    def get_refined(file_name):
        try:
            lst = ['JANSPORT_US', 'KIPLING_US', 'SMARTWOOL_US', 'TIMBERLAND_CAN', 'TIMBERLAND_EU', 'TIMBERLAND_US',
                   'TNF_CAN', 'TNF_CA',
                   'TNF_EU', 'TNF_US', 'VANS_EU', 'VANS_US']
            # lst2 = ['Last_Touch_Channel', 'First_Touch_Channel']
            # lst3 = ['Last_Touch_Weekly', 'First_Touch_Weekly']
            # lst4 = ['Last_Touch_BI_Weekly', 'First_Touch_BI_Weekly']
            res = [ele for ele in lst if (ele in file_name)]
            if res != []:
                res1 = [ele for ele in ['Last_Touch_Channel', 'First_Touch_Channel'] if (ele in file_name)]
                res2 = [ele for ele in ['Last_Touch_Weekly', 'First_Touch_Weekly'] if (ele in file_name)]
                res3 = [ele for ele in ['Last_Touch_BI_Weekly', 'First_Touch_BI_Weekly'] if (ele in file_name)]
                if res1 != []:
                    file = file_name.split('_')
                    refined_file = file[0] + '_' + file[1] + '_ADOBEATTRIBUTION_' + file[2]+'_'+file[3]+'_Channel_'
                    print(refined_file)
                elif res2 != []:
                    file = file_name.split('_')
                    refined_file = file[0] + '_' + file[1] + '_ATTRIBUTIONWEEKLY_' + file[2] + '_' + file[3] + '_Weekly_'
                    print(refined_file)
                elif res3 != []:
                    file = file_name.split('_')
                    refined_file = file[0] + '_' + file[1] + '_ATTRIBUTIONBIWEEKLY_' + file[2] + '_' + file[3] + '_BI_Weekly_'
                    print(refined_file)
                else:
                    file = file_name.split('_')
                    refined_file = file[0]+'_'+file[1]+'_ADOBE_'+file[2]
            elif file_name.find('CATCHPOINT') != -1:
                refined_file = file_name
            elif file_name.find('COREMETRICS') != -1:
                refined_file = file_name
            elif file_name.find('Extract') != -1:
                file = file_name.split(" ")
                refined_file = file[0]+file[1]+'_'+'Data_FORESEE_'+file[4].upper()+'_'+file[5]
                print(refined_file)
            elif file_name.find('GOOGLEANALYTICS') != -1:
                refined_file = file_name
            elif file_name.find('reports') != -1:
                file = file_name.split("-")
                refined_file = file[0]+'_'+file[1]+'_HITWISE_'+file[2]
                print(refined_file)
            elif file_name.find('SINGLE-BRAND') != -1:
                file = file_name.split('_')
                file_part = file[0].split('-')
                refined_file = file_part[0].capitalize()+'_'+file_part[1].capitalize()+'_'+file[1]+'_'+file[2]
            elif file_name.find('STORIES_OUTPUT') != -1:
                file = file_name.split('_')
                file[0] = file[0].capitalize()
                file[1] = file[1].capitalize()
                refined_file = "_".join(file[0:-1])+'_'
                print(refined_file)
            elif file_name.find('TWITTER') != -1:
                refined_file = file_name
            elif file_name.find('YOUTUBE') != -1:
                refined_file = file_name
            elif file_name.find('WeatherTrends'):
                refined_file = file_name
        except Exception as error:
            print(error)
            refined_file = ''
        return refined_file


