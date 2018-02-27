


def name(path):
    if path == '/':
        return '/'
    list_path = path.split('/')
    if len(list_path) == 1:
        return list_path[0]
    else:
        return list_path[-1]
    
#relative_context = returns the context, eg (path = /a/b/c, it returns = /a/b => the context of the c) 
def parent_path(path):
    if path == '/':
        return path
    path_list = path.split('/')
    path_list = path_list[1:]

    if len(path_list) == 1:
        return '/'
    path_list = path_list[:-1]
    for i in range (0, len(path_list)):
        path_list[i] = '/'+ path_list[i]
    path = ''.join(path_list)
    return path
        

def convert_to_list(string_value):  # Function which takes string values and break it down into blocks of 8 byte and store it as a list
    word = ''
    count = 0
    list_value = []
    for i in range (0, len(string_value)):
        word = word + string_value[i]       
        if len(word) == 8:     # As string takes one character as one 1 byte we can also count the length of characters 
            list_value.insert(count, word)
            word = ''
            count += 1
    if len(word) != 0:
        for k in range(0, 8-(len(word))):
            word = word + "\0"
        list_value.insert(count+1, word)   # The remaining values whose size is not exactly 8 bytes
    return list_value