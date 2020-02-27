import xml.etree.ElementTree as ET
from datetime import datetime
from io import StringIO
import csv
import boto3
s3 = boto3.client('s3')
sr3 = boto3.resource('s3')
row_dict = {}
row_dict_list = []

class rf_coremetrics:
    """class with default constructor to perform parsing of coremetrics
        
    """

    @staticmethod
    def parser(source_bucket, file_name):

        try:
            file_parts = file_name.rsplit('/', 1)
            file = file_parts[1].strip('.xml').rsplit('_', 2)
            y = file[0].rsplit('_', 1)
            #date_obj = datetime.strptime(y[1], '%Y%m%d')
            #date = date_obj.strftime('%Y-%m-%d')
            #Reading the Xml file from s3
            obj = sr3.Object(source_bucket, file_name)
            body = obj.get()['Body'].read().decode(encoding="utf-8", errors="ignore")
            #Getting the root element of the Xml file
            root = ET.fromstring(body)
            row_dict_list = []
            view_id_list = []
            metric_list = []
            value_list = []
            #Parsing the Xml file and writing the content in a dictionary
            for item in root.findall('.ToplineResponse/View/viewid'):
                view_id_list.append(item.text)
            for i in range(0, len(view_id_list)):
                row_dict = {}
                row_dict['viewid'] = view_id_list[i]
                row_dict_list.insert(i, row_dict)
            for item in root.findall('.ToplineResponse/Header/PeriodA'):
                startdate = item.attrib.get('startDate')
                endDate = item.attrib.get('endDate')
            for i in range(0, len(row_dict_list)):
                row_dict_list[i].update({'startDate': startdate})
                row_dict_list[i].update({'endDate': endDate})
            for i in root.findall('.ToplineResponse/Data'):
                for j in range(0, 6):
                    for metric in i[j].iter('Metric'):
                        for value in metric.iter('Value'):
                            metric_list.append(metric.attrib.get('id'))
                            value_list.append(value.text)
                            break
            for i in range(0, 87):
                row_dict_list[0][metric_list[i]] = value_list[i]
            for i in range(87, 174):
                row_dict_list[1][metric_list[i]] = value_list[i]
            for i in range(174, 261):
                row_dict_list[2][metric_list[i]] = value_list[i]
            print(row_dict_list)

            #Writing the headers and data into csv file
            f = csv.writer(open('/tmp/COREMETRICS_VANS_CM_.csv', 'a', newline=''))
            f.writerow(
                ["startDate", "endDate", "Device_category", "TL_TOTAL_SALES", "TL_TOTAL_ORDERS", "TL_AVERAGE_ORDER_VALUE",
                 "TL_TOTAL_PRODUCT_VIEWS", "TL_TOTAL_ITEMS_ORDERED", "TL_ITEMS_PER_ORDER", "CART_ABANDONMENT_RATE",
                 "TL_BUYERS_PER_VISITOR", "ORDERS_PER_SESSION", "EVENTS_COMPLETED", "EVENTS_PER_SESSION", "EVENT_POINTS",
                 "EVENT_POINTS_PER_SESSION", "TL_TOTAL_VISITORS", "TL_TOTAL_BUYERS", "UNIQUE_REGISTRANTS", "NEW_VISITORS",
                 "NEW_VISITOR_PCT", "NEW_SESSIONS", "NEW_BUYERS", "NEW_BUYERS_PCT", "REPEAT_VISITORS", "REPEAT_SESSIONS",
                 "REPEAT_BUYERS", "REPEAT_BUYERS_PCT", "TL_TOTAL_PAGE_VIEWS", "TL_TOTAL_SESSIONS", "PAGE_VIEWS_PER_SESSION",
                 "SESSIONS_PER_VISITOR", "BOUNCE_RATE", "TL_AVERAGE_SESSION_LENGTH", "AVERAGE_TIME_PER_PAGE",
                 "ONSITE_SEARCHES", "ELEMENT_VIEWS", "ELEMENT_VIEWS_PER_SESSION", "SERVER_CALLS", "SALES",
                 "ANONYMOUS_SALES",
                 "ORDERS", "ANONYMOUS_ORDERS", "PRODUCT_VIEWS", "ANONYMOUS_PRODUCT_VIEWS", "ITEMS_ORDERED",
                 "ANONYMOUS_ITEMS_ORDERED", "UNIQUE_VISITORS", "EST_ANONYMOUS_VISITORS", "UNIQUE_BUYERS",
                 "EST_ANONYMOUS_BUYERS", "NEW_REGISTRANTS", "REPEAT_REGISTRANTS", "DLOAD_NEW_SESSIONS",
                 "REFERRAL_NEW_SESSIONS",
                 "SEARCH_NEW_SESSIONS", "DLOAD_REPEAT_SESSIONS", "REFERRAL_REPEAT_SESSIONS", "SEARCH_REPEAT_SESSIONS",
                 "PAGE_VIEWS", "ANONYMOUS_PAGE_VIEWS", "SESSIONS", "EST_ANONYMOUS_SESSIONS", "AVG_NEW_SESSION_LENGTH",
                 "AVG_REPEAT_SESSION_LENGTH", "CNT_LINKIMPR", "CNT_ELEMENTS", "CNT_CONVEVENTS", "CNT_SHOPACT",
                 "CNT_CUSTOMEVENTS", "DLOAD_SESSIONS", "REFERRAL_SESSIONS", "SEARCH_SESSIONS", "TOTAL_MOBILE_SESSIONS",
                 "TOTAL_MOBILE_SESSIONS_PCT"])
            for j in range(0, len(row_dict_list)):
                f.writerow([row_dict_list[j]['startDate'], row_dict_list[j]['endDate'], row_dict_list[j]['viewid'],
                            row_dict_list[j]['TL_TOTAL_SALES'], row_dict_list[j]['TL_TOTAL_ORDERS'],
                            row_dict_list[j]['TL_AVERAGE_ORDER_VALUE'],
                            row_dict_list[j]['TL_TOTAL_PRODUCT_VIEWS'], row_dict_list[j]['TL_TOTAL_ITEMS_ORDERED'],
                            row_dict_list[j]['TL_ITEMS_PER_ORDER'],
                            row_dict_list[j]['CART_ABANDONMENT_RATE'], row_dict_list[j]['TL_BUYERS_PER_VISITOR'],
                            row_dict_list[j]['ORDERS_PER_SESSION'],
                            row_dict_list[j]['EVENTS_COMPLETED'], row_dict_list[j]['EVENTS_PER_SESSION'],
                            row_dict_list[j]['EVENT_POINTS'],
                            row_dict_list[j]['EVENT_POINTS_PER_SESSION'], row_dict_list[j]['TL_TOTAL_VISITORS'],
                            row_dict_list[j]['TL_TOTAL_BUYERS'],
                            row_dict_list[j]['UNIQUE_REGISTRANTS'], row_dict_list[j]['NEW_VISITORS'],
                            row_dict_list[j]['NEW_VISITOR_PCT'],
                            row_dict_list[j]['NEW_SESSIONS'], row_dict_list[j]['NEW_BUYERS'],
                            row_dict_list[j]['NEW_BUYERS_PCT'],
                            row_dict_list[j]['REPEAT_VISITORS'], row_dict_list[j]['REPEAT_SESSIONS'],
                            row_dict_list[j]['REPEAT_BUYERS'],
                            row_dict_list[j]['REPEAT_BUYERS_PCT'], row_dict_list[j]['TL_TOTAL_PAGE_VIEWS'],
                            row_dict_list[j]['TL_TOTAL_SESSIONS'],
                            row_dict_list[j]['PAGE_VIEWS_PER_SESSION'], row_dict_list[j]['SESSIONS_PER_VISITOR'],
                            row_dict_list[j]['BOUNCE_RATE'],
                            row_dict_list[j]['TL_AVERAGE_SESSION_LENGTH'], row_dict_list[j]['AVERAGE_TIME_PER_PAGE'],
                            row_dict_list[j]['ONSITE_SEARCHES'],
                            row_dict_list[j]['ELEMENT_VIEWS'], row_dict_list[j]['SERVER_CALLS'], row_dict_list[j]['SALES'],
                            row_dict_list[j]['ANONYMOUS_SALES'], row_dict_list[j]['ORDERS'],
                            row_dict_list[j]['ANONYMOUS_ORDERS'],
                            row_dict_list[j]['PRODUCT_VIEWS'], row_dict_list[j]['ANONYMOUS_PRODUCT_VIEWS'],
                            row_dict_list[j]['ITEMS_ORDERED'],
                            row_dict_list[j]['ANONYMOUS_ITEMS_ORDERED'], row_dict_list[j]['UNIQUE_VISITORS'],
                            row_dict_list[j]['UNIQUE_VISITORS'], row_dict_list[j]['EST_ANONYMOUS_VISITORS'],
                            row_dict_list[j]['UNIQUE_BUYERS'], row_dict_list[j]['EST_ANONYMOUS_BUYERS'],
                            row_dict_list[j]['NEW_REGISTRANTS'],
                            row_dict_list[j]['REPEAT_REGISTRANTS'], row_dict_list[j]['DLOAD_NEW_SESSIONS'],
                            row_dict_list[j]['REFERRAL_NEW_SESSIONS'],
                            row_dict_list[j]['SEARCH_NEW_SESSIONS'], row_dict_list[j]['DLOAD_REPEAT_SESSIONS'],
                            row_dict_list[j]['REFERRAL_REPEAT_SESSIONS'],
                            row_dict_list[j]['SEARCH_REPEAT_SESSIONS'], row_dict_list[j]['PAGE_VIEWS'],
                            row_dict_list[j]['ANONYMOUS_PAGE_VIEWS'],
                            row_dict_list[j]['SESSIONS'], row_dict_list[j]['EST_ANONYMOUS_SESSIONS'],
                            row_dict_list[j]['AVG_NEW_SESSION_LENGTH'],
                            row_dict_list[j]['AVG_REPEAT_SESSION_LENGTH'], row_dict_list[j]['CNT_LINKIMPR'],
                            row_dict_list[j]['CNT_ELEMENTS'],
                            row_dict_list[j]['CNT_CONVEVENTS'], row_dict_list[j]['CNT_SHOPACT'],
                            row_dict_list[j]['CNT_CUSTOMEVENTS'],
                            row_dict_list[j]['DLOAD_SESSIONS'], row_dict_list[j]['REFERRAL_SESSIONS'],
                            row_dict_list[j]['REFERRAL_SESSIONS'],
                            row_dict_list[j]['TOTAL_MOBILE_SESSIONS'], row_dict_list[j]['TOTAL_MOBILE_SESSIONS']])
        except Exception as error:
            print(error)
        return '/tmp/COREMETRICS_VANS_CM_.csv', file[2]
