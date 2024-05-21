import argparse
import random
import re
import socket
import threading
import urllib.request
from time import time

import socks

user_agents = []
with open("user_agents.txt", "r") as f:
    for line in f:
        user_agents.append(line.replace("\n", ""))


class Proxy:
    def __init__(self, method, proxy):
        if method.lower() not in ["http", "https", "socks4", "socks5"]:
            raise NotImplementedError("Only HTTP, HTTPS, SOCKS4, and SOCKS5 are supported")
        self.method = method.lower()
        self.proxy = proxy

    def is_valid(self):
        return re.match(r"\d{1,3}(?:\.\d{1,3}){3}(?::\d{1,5})?$", self.proxy)

    def check(self, site, timeout, user_agent, verbose):
        if self.method in ["socks4", "socks5"]:
            socks.set_default_proxy(socks.SOCKS4 if self.method == "socks4" else socks.SOCKS5,
                                    self.proxy.split(':')[0], int(self.proxy.split(':')[1]))
            socket.socket = socks.socksocket
            try:
                start_time = time()
                urllib.request.urlopen(site, timeout=timeout)
                end_time = time()
                time_taken = end_time - start_time
                verbose_print(verbose, f"Proxy {self.proxy} is valid, time taken: {time_taken}")
                return True, time_taken, None
            except Exception as e:
                verbose_print(verbose, f"Proxy {self.proxy} is not valid, error: {str(e)}")
                return False, 0, e
        else:
            url = self.method + "://" + self.proxy
            proxy_support = urllib.request.ProxyHandler({self.method: url})
            opener = urllib.request.build_opener(proxy_support)
            urllib.request.install_opener(opener)
            req = urllib.request.Request(self.method + "://" + site)
            req.add_header("User-Agent", user_agent)
            try:
                start_time = time()
                urllib.request.urlopen(req, timeout=timeout)
                end_time = time()
                time_taken = end_time - start_time
                verbose_print(verbose, f"Proxy {self.proxy} is valid, time taken: {time_taken}")
                return True, time_taken, None
            except Exception as e:
                verbose_print(verbose, f"Proxy {self.proxy} is not valid, error: {str(e)}")
                return False, 0, e

    def __str__(self):
        return self.proxy


def verbose_print(verbose, message):
    if verbose:
        DEFAULT = '\033[0m'
        GREEN = '\033[92m'
        YELLOW = '\033[93m'
        WHITE = '\033[97m'
        echo = lambda values, color: print("%s%s%s" % (color, values, DEFAULT)) if color \
            else print("%s%s" % (values, DEFAULT))
        if 'not valid' in message:
            echo(message, color=WHITE)
        else:
            echo(message, color=GREEN)

def _check(file, timeout, method, site, verbose, random_user_agent):
    proxies = []
    with open(file, "r") as f:
        for line in f:
            proxies.append(Proxy(method, line.replace("\n", "")))

    print(f"Checking {len(proxies)} proxies")
    proxies = filter(lambda x: x.is_valid(), proxies)
    valid_proxies = []
    user_agent = random.choice(user_agents)

    def check_proxy(proxy, user_agent):
        new_user_agent = user_agent
        if random_user_agent:
            new_user_agent = random.choice(user_agents)
        valid, time_taken, error = proxy.check(site, timeout, new_user_agent, verbose)
        valid_proxies.extend([proxy] if valid else [])

    threads = []
    for proxy in proxies:
        t = threading.Thread(target=check_proxy, args=(proxy, user_agent))
        threads.append(t)

    for t in threads:
        t.start()

    for t in threads:
        t.join()

    with open(file, "w") as f:
        for proxy in valid_proxies:
            f.write(str(proxy) + "\n")

    print(f"Found {len(valid_proxies)} valid proxies")


def checker(timeout=20, proxy='http', file='http.txt', site='https://screener.in/', verbose=False, random_agent=False):
    _check(file=file, timeout=timeout, method=proxy, site=site, verbose=verbose, random_user_agent=random_agent)