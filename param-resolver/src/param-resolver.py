#! /usr/bin/python

import boto3
import json
import os

ssm_client = boto3.client('ssm')

def parse_template_json():
    with open('templates.txt', 'r') as file_locs:
        for abspath in file_locs.readlines():
            abspath = abspath.strip('\n')
            parent_path = '/'.join(abspath.split('/')[:-1])
            with open(abspath, 'r') as intemplate, open(f'{parent_path}/template-resolved.json', 'w+') as outtemplate:
                template_json = json.load(intemplate)
                environment_map = template_json.get('Mappings').get('EnvironmentMap')
                for env, env_vars_obj in environment_map.items():
                    param_store_json = ssm_client.get_parameter(Name=env).get('Parameter').get('Value')
                    param_store_json = json.loads(param_store_json)
                    for var_name, val in env_vars_obj.items():
                        if isinstance(val, str):
                            new_val = param_store_json.get(var_name)
                            template_json['Mappings']['EnvironmentMap'][env][var_name] = new_val

                json.dump(template_json, outtemplate, indent=4)
                # print(template_json)
                # print(template_json)

if __name__ == '__main__':
    parse_template_json()
