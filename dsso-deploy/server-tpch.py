import argparse
import socket

import numpy as np
import torch
import torch.nn.functional as F

import os

from preprocessor import Preprocessor
from multiprocessing.dummy import Pool as ThreadPool

from lstm import LSTMNet
from datetime import datetime
import pickle

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-ip", type=str, nargs='?', default="11.167.226.172") # ip
    parser.add_argument("-p", type=str, nargs='?', default="8308") # port
    parser.add_argument("-m", type=str, nargs='?', default="./tpch/model-tpch.pt") # model weights
    parser.add_argument("-d", type=str, nargs='?', default="./tpch") # metadata dir

    args = parser.parse_args()

    device = 'cuda'

    server = socket.socket()
    server.bind((args.ip,int(args.p)))
    server.listen()

    ## preparation
    dir_path = os.path.dirname(os.path.realpath(__file__))
    os.chdir(dir_path)

    if args.d.endswith("/"):
        d = args.d
    else: d = args.d + "/"
    max_resource = {'n_executor': 6, 'g_mem': 16, 'n_core':3, 'n_worker':3}

    seq_len = 47
    col_len = 1230
    condition_len = 20
    table_len = len(pickle.load(open(d+'table_onehot_tpch.pkl', 'rb'))) + 2
    operation_len = len(pickle.load(open(d+'operation_onehot_tpch.pkl', 'rb')))
    lstm_input_dim = seq_len + col_len + condition_len + table_len + operation_len

    preprocessor = Preprocessor(d+'column_onehot_tpch.pkl', \
        d+'table_onehot_tpch.pkl', \
        d+'operation_onehot_tpch.pkl',\
        d+"tpch30-metadata.json",\
        max_resource,\
        d+"usif_tpch",\
        seq_len,col_len,condition_len)

    net = LSTMNet(lstm_input_dim, 256, 256, 2, auxi_len=5)
    net.load_state_dict(torch.load(args.m, map_location=torch.device(device)))
    net.eval()

    while True:
        conn, addr = server.accept()
        data = ""
        while True:
            d= conn.recv(1024)
            if(len(d) == 0): 
                break
            decode = d.decode()
            data += decode
            if decode.endswith("done"):
                break
        # print("== Recieved json: "+ data.decode())
        if(len(d) == 0): 
            continue
        data_org = data
        try:
            # parse input
            with open('test_input', 'w') as f:
                f.write(data)
            data  = [i for i in data.split("\n") if i !='']
            resources = data[0]
            lines = data[1:]
            plans = []
            for l in lines:
                if(l[0] == "="):
                    plans.append(l+'\n')
                else:
                    plans[-1] += l+'\n'
            resourceArr = [float(i) for i in resources.strip()[1:-1].split(',')]
            resource = {'n_executor': resourceArr[0], 'g_mem': resourceArr[1], 'n_core':resourceArr[2], 'n_worker':resourceArr[3]}
            normalized_resources = preprocessor.normalize_resource(resource)
            preprocessor.normalized_resources = normalized_resources
            pool = ThreadPool(32)
            encoded = pool.map(preprocessor.parse_plan_padding, plans)
            pool.close()
            pool.join()

            # inference
            plans_list = encoded

            inputs = torch.stack([i[0] for i in plans_list])
            lens = torch.vstack([i[1] for i in plans_list])
            resources = torch.vstack([i[2] for i in plans_list])

            lens_n_resources = torch.hstack((lens.reshape((-1, 1)), resources))
            lens_n_resources_padded = lens_n_resources.reshape((-1, 1, 5))
            lens_n_resources_padded = F.pad(lens_n_resources_padded, (0, inputs.shape[-1]-5), 'constant', 0)
            inputs = torch.cat((inputs, lens_n_resources_padded), 1).to(device)
            output = net.forward(inputs)
            res = output[0].cpu().detach().numpy()
            res = [i[0] for i in res]
            print(res)
            print('number of plans: {}'.format(len(plans)))
            best_idx = str(np.array(res).argmin())
            print("best idx: {}".format(best_idx))

            # send result
            conn.send(str.encode("best idx: {}\n".format(best_idx)))
            conn.send(str.encode("done\n"))
                
        except:
            print('failed to predict plan text')
            conn.send(b'failed to predict plan: \n')
            conn.send(str.encode("done\n"))
            time_str = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
            with open("input-{}".format(time_str), 'w') as f:
                f.write(data_org)

