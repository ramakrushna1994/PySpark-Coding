def remspecialchar(value):
    import re
    
    # Using regex to find the part to replace
    transformed_value = re.sub(r'Pathway\s*-\s*\d*X', 'Pathway X', value)
    
    # Replacing the parentheses with a space
    transformed_value = re.sub(r'\s*\(\s*', ' ', transformed_value).replace(')', '').strip()
    
    return transformed_value

'''
Example usage

input_value = "[Indiana Individual Off Exchange Network] Pathway HMO POS"
output_value = remspecialchar(input_value)
print(output_value)  # Output should be "Pathway X with dental"

'''


