
data = {}

with open("./Datasets/user_business/user_business.csv", "r") as f:
    for i in f.readlines():
        user, business = i.strip().split(',')
        try:
            data[business].append(user)
        except:
            data[business] = [user]

return data.values()