import pandas as pd 

df = pd.DataFrame({"x":[1, 2, 3, 4, 5], 
                        "y":[3, 4, 5, 6, 7]}, 
                      index=['a', 'b', 'c', 'd', 'e'])

print(df.where(df['x'] > 3,df['x'],axis = 1))