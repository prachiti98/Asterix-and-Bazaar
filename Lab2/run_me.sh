python3 peer.py 1 10030 '{"Role": "Buyer","Inv":{},"shop":["Fish","Salt","Boar"]}'  7 &
python3 peer.py 2 10031 '{"Role": "Seller","Inv":{"Fish":0,"Boar":3,"Salt":0},"shop":{}}' 7 &
python3 peer.py 3 10032 '{"Role": "Buyer","Inv":{},"shop":["Salt","Boar","Fish"]}' 7 & 
python3 peer.py 4 10033 '{"Role": "Seller","Inv":{"Fish":3,"Boar":0,"Salt":0},"shop":{}}' 7 &  
python3 peer.py 5 10034 '{"Role": "Buyer","Inv":{},"shop":["Boar","Fish","Salt"]}' 7 &
python3 peer.py 6 10035 '{"Role": "Seller","Inv":{"Fish":0,"Boar":0,"Salt":3},"shop":{}}' 7 &
python3 peer.py 7 10036 '{"Role": "Seller","Inv":{"Fish":0,"Boar":0,"Salt":3},"shop":{}}' 7 &