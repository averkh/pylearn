# Первая цифра  двухзначного числа

n = int(input())

#print('Первая цифра числа', n // 10)

if n in range(0,9):
    print('Дурак чтоль, это цифра')
else:
    if n in range(10,100):
        print(n // 10)
    elif n in range(100,1000):
        print(n // 100)
    elif n in range(1000,10000):
        print(n // 1000)
