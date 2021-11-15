# This silly script exists because you can't call a within-module file to start a program, you gotta import it
# first with python =(
import lsw_slackbot
import asyncio

lsw_slackbot.client_loop()
