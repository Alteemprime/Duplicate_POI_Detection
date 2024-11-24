import json
import sys

def load_parameters(parameter_file = 'parameters.json'):
    with open(parameter_file,'r') as file:
        return json.load(file)
    
def update_parameters(parameter_file = 'parameters.json',**update_items):
    try:
        with open(parameter_file, 'r') as file:
            parameters = json.load(file)
    except FileNotFoundError:
        print('parameter.json not found')
        sys.exit(1)
        
    for key,values in update_items.items():
        parameters[key] = values
        
    with open (parameter_file, 'w') as file:
        json.dump(parameters, file, indent = 4)