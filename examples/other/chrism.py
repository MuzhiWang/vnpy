import numpy as np

# 定义状态转移矩阵 P，状态顺序: 0,1,2,3,4,5,6,7
up_1 = 0.08
up_2 = 0.012
down = 0.05
stay_1 = 1 - down - up_1
stay_2 = 1 - down - up_2
P = np.array([
    [0.05, 0.95, 0, 0, 0, 0, 0, 0],
    [down, stay_1, up_1, 0, 0, 0, 0, 0],
    [0, down, stay_1, up_1, 0, 0, 0, 0],
    [0, 0, down, stay_2, up_2, 0, 0, 0],
    [0, 0, 0, down, stay_2, up_2, 0, 0],
    [0, 0, 0, 0, down, stay_2, up_2, 0],
    [0, 0, 0, 0, 0, down, stay_2, up_2],
    [0, 0, 0, 0, 0, 0, 0, 1.0 ]
])

# 初始状态：100% 在幸运0
p0 = np.array([1,0,0,0,0,0,0,0], dtype=float)

# 要计算的瓶数
N_list = [100, 500, 1000, 2000, 3000, 4000, 5000, 8000, 10000, 20000, 30000]

# 逐步计算状态分布
p = p0.copy()
results = {}
for n in range(1, max(N_list)+1):
    p = p @ P
    if n in N_list:
        results[n] = p[-1]   # p[-1] 为吸收态的概率

for n in N_list:
    print(f"N = {n}: 至少达到一次幸运7的概率 ≈ {results[n]*100:.1f}%")
