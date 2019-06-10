model_module = __import__('proxy.Sample', fromlist='Sample')
print(model_module.Sample(19))
# s=eval('Sample(19)')
# print(type(s))