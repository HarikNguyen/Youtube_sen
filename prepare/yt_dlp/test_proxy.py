import requests

def verify_proxy_connection():
    my_proxy = "http://pcVyRk0aXV-res-vn:PC_4Ywuqw5KzJEOLJJwM@proxy-us.proxy-cheap.com:5959"
    
    proxy_settings = {
        "http": my_proxy,
        "https": my_proxy
    }
    
    print("Step 1: Checking your real home IP address...")
    real_ip = requests.get("https://api.ipify.org").text
    print("Your Home IP: " + real_ip)
    print("--------------------------------------------------")
    
    print("Step 2: Sending traffic through the proxy...")
    try:
        proxy_ip = requests.get("https://api.ipify.org", proxies=proxy_settings, timeout=10).text
        print("Your Proxy IP: " + proxy_ip)
        print("--------------------------------------------------")
        
        if real_ip != proxy_ip:
            print("Status: SUCCESS! Your proxy is working correctly and hiding your identity.")
        else:
            print("Status: WARNING! The IP did not change. The proxy might not be connecting.")
            
    except Exception as error:
        print("Status: FAILED. Could not connect to the proxy.")
        print("Error details: " + str(error))

if __name__ == "__main__":
    verify_proxy_connection()
