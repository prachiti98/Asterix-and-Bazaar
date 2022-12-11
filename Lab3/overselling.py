import matplotlib.pyplot as plt
import numpy as np


ratioOfBuyerAndSeller = np.array([0.5,0.666,1,1.5,2,2.5,3])
Percentage = np.array([0,11,15,36,48,50,54])
# plot graph
plt.plot(ratioOfBuyerAndSeller, Percentage) 


plt.title('Ratio of Buyer and Seller v/s Percentage of Overselling')
plt.ylabel('Percentage (in %)')
plt.xlabel("Ratio of Buyers and Sellers")

plt.show()
