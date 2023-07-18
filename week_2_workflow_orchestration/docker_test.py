#!/usr/bin/env python
# coding: utf-8

from prefect import flow, task

    
@task(log_prints=True, retries = 3)
def print_name():
    print("Hey there Mownica!")
    return None


@task(log_prints= True, retries = 3)
def greetings():
   print("How are you doing??!")
   return None

@flow(name = 'data_flow')
def main_flow():
    print_name()
    greetings()

if __name__ == '__main__':
    main_flow()

    
   