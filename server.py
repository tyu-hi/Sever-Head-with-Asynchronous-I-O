import asyncio # asyncio = Asynchronous I/O
import json
import aiohttp
import logging
import sys
import time
import config # important values

"""

This Python code defines a server application that handles messages received over a network, demonstrating asyncrhonous communication and message handling between servers. 
Here's a summary of its functionality:
It imports necessary modules for asynchronous I/O (asyncio), JSON manipulation (json), HTTP requests (aiohttp), logging, system operations (sys), time functions (time), and a custom configuration file (config).
It sets up important values like the Google Places API key, the server ID obtained from command line arguments, and the local host address.
It defines a dictionary mapping server names to their corresponding ports and a dictionary specifying the communication flow between servers.
It defines a Server class with methods for handling different types of input messages (IAMAT, AT, WHATSAT) and for communicating with the Google Places API to retrieve information about nearby places.
The handleInput method is responsible for processing incoming messages, decoding them, and calling the appropriate handler method based on the message type.
The handleIAMAT method handles the IAMAT messages, calculates the time difference between the server's receipt of the message and the client's sending of the message, and responds with an AT message.
The handleAT method handles AT messages received from other servers, updates the server's client history, and propagates the message to other servers in the network.
The handleWHATSAT method handles WHATSAT messages, retrieves place information from the Google Places API, and responds with a message containing the requested information.
The propagateMessage method is used to propagate messages to neighboring servers in the network.
The runServer method starts the server and listens for incoming connections, using the asyncio framework.
The main function parses command line arguments to determine the server ID, creates a Server object, and runs the server.
"""
# Google Places API
APIKEY = config.GOOGLE_PLACES_API_KEY
# Obtain inputed server id
serverID = sys.argv[1]
# Localhost address
LOCALHOST = '127.0.0.1'

# test using telnet
# clark: telnet 127.0.0.1 21451
# Bailey: telnet 127.0.0.1 21448
# Bona: telnet 127.0.0.1 21449
# Campbell: telnet 127.0.0.1 21450
# Jaquez: telnet 127.0.0.1 21452
# IAMAT kiwi.cs.ucla.edu +34.068930-118.445127 1621464827.959498503
# IAMAT apple.cs.ucla.edu +34.068930-118.445127 1621464827.959498503
# WHATSAT kiwi.cs.ucla.edu 10 5

# These are the ports obtained from TA ports spreadsheet. These are specific to me.
servers = { 
    'Bailey': 21448,
    'Bona': 21449,
    'Campbell': 21450,
    'Clark': 21451,
    'Jaquez': 21452
}

"""
Servers communicate with each other bidirectionally, in this pattern:
    Clark talks with Jaquez and Bona.
    Campbell talks with everyone else but Clark.
    Bona talks with Bailey.
"""
networkFlow = {
    'Clark': ['Jaquez', 'Bona'],
    'Campbell': ['Bailey', 'Bona', 'Jaquez'],
    'Bona': ['Clark', 'Campbell', 'Bailey'],
    'Jaquez': ['Clark', 'Campbell'],
    'Bailey': ['Campbell', 'Bona']
}


class Server:
    def __init__(self, id, ip=LOCALHOST):

        self.port = servers[id]
        self.id = id # current server in use
        self.ip = ip

        #self.handleAPI = HandleAPI()
        #self.checkFormat = CheckFormat()

        """
        client can send its location to server in this format:
            IAMAT kiwi.cs.ucla.edu +34.068930-118.445127 1621464827.959498503

        server should resepond to clients with a message in this format:
            AT Clark +0.263873386 kiwi.cs.ucla.edu +34.068930-118.445127 1621464827.959498503

        clients can query for infomration about places near client's loctions in this format:
            WHATSAT kiwi.cs.ucla.edu 10 5
        """

        """
        AT Clark +0.263873386 kiwi.cs.ucla.edu +34.068930-118.445127 1621464827.959498503
        client messsage format
        serverMessage[0] = 'AT' 
        serverMessage[1] = serverID
        serverMessage[2] = time difference between the server's idea of when it 
                           got the message from the client and the client's time stamp
        serverMessage[3] = clientID
        serverMessage[4] = latittude and longitude in ISO 6709 notation
        serverMessage[5] = client's idea of when it sent messsage in POSIX time.
        """
        # store timestamps of received messages and stuff
        self.clientHistory = dict()
        self.clientMessage = dict()

        # creater server logs 
        # self.logFile = open(self.id + '.log', 'w', 1)
        logging.basicConfig(filename=f'{self.id}.log', format='%(levelname)s: %(message)s', filemode='w+', level=logging.INFO)
        logging.info("Log created for server {}".format(self.id))
        # self.logFile.write("hello")
        # remember to close this log in 

    ############### HANDLING INPUT MESSAGES ##################################
    async def handleInput(self, reader, writer):
        # Server receives input from telnet message

        # Piazza says coordinates will be valid.
        # exist when buffer is not empty
        # while not reader.at_eof(): 
            # waiat for server to get data

        data = await reader.readline()

        # get server recepptioon time
        timeReceivedByServer = time.time()
            
        try:
            message = data.decode()
        except UnicodeDecodeError as e:
            # Log the decoding error
            logging.info("UnicodeDecodeError: {}\n".format(e))
            # Optionally, you could log the raw data or handle it in another way
            logging.info("Received raw data: {}\n".format(data))

        # ignore empty "" messages
        if message == "": 
            logging.info("Empty message.")
            pass
        
        # split message prompt from telnet into sections
        prompt = message.split()
        
        # tests
        print(f"Received message: {message}")
        print(f"Split message: {prompt}")
        
        # server received IMAT/AT/WHATSAT messagee from telnet
        logging.info("{} received new message: {}".format(self.id, message))

        # handle inputs
        if len(prompt) != 4:
            # this is a irregular command
            # handling response AT message
            # responseMessage = await self.handleIrregularCommand(prompt, message)
            responseMessage = await self.handleAT(prompt, message)
            print("AT message")
        elif prompt[0] == "IAMAT":
            print("Handling IAMAT message")
            responseMessage = await self.handleIAMAT(prompt, timeReceivedByServer)
        elif prompt[0] == "WHATSAT":
            print(f"Handling WHATSAT message: {prompt}")
            responseMessage = await self.handleWHATSAT(prompt)
            #responseMessage = await self.handle_whatsat_command(prompt)
        else:
            # Servers should respond to invalid commands with a line that contains 
            # a question mark (?), a space, and then a copy of the invalid command.
            responseMessage = "? " + message
    
        if responseMessage != None:
            logging.info("Responding to client with: {}". format(responseMessage))
            writer.write(responseMessage.encode())
            await writer.drain()

        logging.info("Closing client socket...\n")
        writer.close()
    
    async def handleAT(self, serverMessage, prompt):
        """
        The server sends AT message to client. Then, we need to propagate this message to all connected servers in the network.

        """
        if len(serverMessage) == 6 and serverMessage[0] == "AT":
            # Servers receive the propgated AT message from the server clinet sent this to 
            # await self.handleAT(message)
            logging.info("Receiving propogated message.")
            message = None

            # This message is already in client history as a propogated message
            if serverMessage[3] in self.clientHistory and serverMessage[5] <= self.clientHistory[serverMessage[3]][5]:
                logging.info("This message is already in the data history for client: {}".format(serverMessage[3]))
                return
            
            # update/add client info
            self.clientHistory[serverMessage[3]] = serverMessage
            logging.info("Updating client history with new AT message for client: {}".format(serverMessage[3]))

            # propogate location to servers
            message = " ".join(serverMessage)
            
            logging.info("Propagating message to other servers...")
            await asyncio.create_task(self.propagateMessage(message))
            logging.info("Received message. Stopping propagation...")
        else:
            # invalid message
            message = "? " + prompt

        return message
    
    async def handleIAMAT(self, clientMessage, timeReceivedbyServer):
        """
        ex: IAMAT kiwi.cs.ucla.edu +34.068930-118.445127 1621464827.959498503
        The client sends IAMAT to server.
        The server should handle te IAMAT by sending AT to client.
        
        Format of IAMAT:
        clientMessage[0] = 'IAMAT'
        clientMessage[1] = clientID
        clientMessage[2] = lat and long in ISO 6709
        clientMessage[3] = POSIX time of when cilent sent message

        Format of AT:
        serverResponseMessage[0] = 'AT'
		serverResponseMessage[1] = server_id
		serverResponseMessage[2] = (time of message received by server) - message_list[3]
		serverResponseMessage[3] = clientMessage[1]
		serverResponseMessage[4] = clientMessage[2]
		serverResponseMessage[5] = clientMessage[3]
        """
        if (self.validIAMAT(clientMessage)):
            print("Valid IAMAT received. Creating server response...")
            serverResponseMessage = []
            serverResponseMessage.append('AT')
            serverResponseMessage.append(self.id)
            serverResponseMessage.append(self.calculateTimeDifference(timeReceivedbyServer, float(clientMessage[3])))
            serverResponseMessage.append(clientMessage[1])
            serverResponseMessage.append(clientMessage[2])
            serverResponseMessage.append(clientMessage[3])

            self.clientHistory[clientMessage[1]] = serverResponseMessage

            serverResponse = " ".join(serverResponseMessage)

            # check thisC!!!!!!!!!!!
            await self.propagateMessage(serverResponse)
        else: 
            # Servers should respond to invalid commands with a line that contains a question mark (?), a space, and then a copy of the invalid command.
            print("Invalid message received.")
            serverResponse = "? " + " ".join(clientMessage)

        return serverResponse

    ##### handle Google API for WHATSAT #####
    
    def convertLocationToCoords(self, location):
        # add comma to divide lat and long coords
        return location.replace("+", " + ").replace("-", " - ").split()
    
    async def fetch(self, session, url):
        async with session.get(url) as response:
            return await response.text()
        
    async def handleGooglePlaces(self, location, radius, bound):
        # a radius (in kilometers) from the client (e.g., 10), 
        # and an upper bound on the amount of information to receive from Places data within that radius of the client (e.g., 5)
        # using aiohttp

        # ClientSession is an interface for making HTTP requests
        async with aiohttp.ClientSession() as session:
            # get coordinates from location 
            # coordinates = self.convertLocationToCoords(location)
            coordinates = location
            if coordinates is None:
                logging.info("Invalid coordinates format.")
                sys.exit(1)
            
            logging.info("Retrieving nearby places from Google at location: {}".format(coordinates))
            
            url = f"https://maps.googleapis.com/maps/api/place/nearbysearch/json?location={coordinates}&radius={radius}&key={APIKEY}"
            result = await self.fetch(session, url)
            jsonResult = json.loads(result)

            # check number of information is within upper bound
            if len(jsonResult["results"]) <= int(bound):
                # numer of results is less than bound, return as is.
                # logging.info("Setting bound on results of: {}".format(bound))
                logging.info("Retrieve from Google successful. Number of results: {}".format(len(jsonResult["results"])))
                return result
            else:
                # apply bound
                logging.info("Retrieve from Google successful. Total number of results: {}".format(len(jsonResult["results"])))
                jsonResult["results"] = jsonResult["results"][0:int(bound)]
                logging.info("Number of results after applying bound: {}".format(len(jsonResult["results"])))
                return json.dumps(jsonResult, sort_keys=True, indent=4)
    
    #### HANDLE INPUTS CONTINUED ####

    async def handleWHATSAT(self, prompt):
        """

        Format of WHATSAT:
        clientMessage[0] = 'WHATSAT'
        clientMessage[1] = clientID
        clientMessage[2] = radius
        clientMessage[3] = upper bound on amount of into to receive from Places data within radius

        Format of server response using AT in the same format as before:
            Following the AT message is a JSON-format message, exactly in the same format that Google Places gives for a Nearby Search request 
            (except that any sequence of two or more adjacent newlines is replaced by a single newline and that all trailing newlines are removed), followed by two newlines. 
        serverResponseMessage[0] = 'AT'
        serverResponseMessage[1] = serverID
        serverResponseMessage[2] = time diff
        serverResponseMessage[3] = clientMessage[1]
        serverResponseMessage[4] = lat and long
        serverResponseMessage[5] = time
        serverResponseMessage[6] = JSON from Google Places

        # Piazza says this will always be a valid message
        """
        # parse WHATSAT message
        print(f"Handling WHATSAT For client {prompt[1]}")
        
        """
        if clientID not in self.clientMessage:
            print(f"client messaeges {self.clientMessage.keys()}")
            # Handle the case where the clientID is not found
            logging.info("Client ID {} not found in records".format(clientID))
            return 
        """
        
        #location = self.clientMessage[prompt[1]].split()[4]
        #radius = prompt[2]
        #bound = prompt[3]

        ### version2
        if (self.validWHATSAT(prompt)):
            logging.info(f"Constructing server response to client query: {self.clientHistory[prompt[1]][:6]}...")

            # get the AT message from client history
            clientATMessage = self.clientHistory[prompt[1]][:6]
            
            # parse AT message for location
            locationList = self.convertLocationToCoords(clientATMessage[4])
            coordinates = '{}{},{}{}'.format(locationList[0], locationList[1], locationList[2], locationList[3])
            # km -> m, since Google API uses meters for radius
            radius = float(prompt[2]) * 1000
            bound = prompt[3]

            # await Google Places API fetch
            places = await self.handleGooglePlaces(coordinates, radius, bound)
            
            # Any sequence of two or more adjacent newlines is replaced by a single newline and that all trailing newlines are removed), 
            # followed by two newlines
            responseMessage = "{}\n{}\n\n".format(clientATMessage, str(places).rstrip('\n'))
        else:
            print("Invalid query received.")
            responseMessage = "? " + " ".join(prompt)
        
        return responseMessage
        
    #### validate messages ####
    def validIAMAT(self, prompt):
        # check if 3rd argument coordinate is in ISO 6709 notation
        # obtain coordinates from prompt message by replacing + with - for easier slitting.
        # Example ISO 6709 lat, long: +34.068930-118.445127
        coordinates = prompt[2].replace('+', '-')
        # spilt at - and filter empty strings
        numbersOnly = list(filter(None, coordinates.split('-')))

        # print(prompt)
        print(f"coords: {coordinates}")
        print(f"numbers only: {numbersOnly}")

        if len(numbersOnly) != 2 or not (isinstance(numbersOnly[0], float) and isinstance(numbersOnly[1]), float):
            print("false numbersOnly length")
            return False

        # check if 4th argument time is a float
        try:
            float(prompt[3])
            pass
        except ValueError:
            print("false data time")
            return False
        
        return True
        
        """  
        if not isinstance(prompt[3], float):
            print(f"time: {prompt[3]}")
            print("false time data type")
            return False
        """
       
    def validWHATSAT(self, prompt):
        # check validilty of WHATSAT message
        # The radius must be at most 50 km, and the information bound must be at most 20 items, 
        # since that's all that the Places API supports conveniently

        if prompt[1] not in self.clientHistory:
            logging.info("WHATSAT Error: Client not in history.")
            return False
        if int(prompt[2]) < 0 or int(prompt[2]) > 50:
            logging.info("WHATSAT Error: Invalid radius.")
            return False
        if int(prompt[3]) < 0 or int(prompt[3]) > 20:
            logging.info("WHATSAT Error: Invalid bound.")
            return False
        
        return True
    
    def calculateTimeDifference(self, timeMessageReceivedByServer, timeMessageReceivedByClient):
        timeDiff = timeMessageReceivedByServer - timeMessageReceivedByClient
        if timeDiff > 0:
            # add + in front of non-zero pos numbers for POSIX time
            timeDiff = '+{}'.format(timeDiff)
        else:
            timeDiff = str(timeDiff)
        
        return timeDiff
    
     ########## RUN SERVER ######################################
    async def runServer(self):
        logging.info("Starting {} server...".format(self.id))
        
        # start server
        server = await asyncio.start_server(self.handleInput, self.ip, self.port) # built-in
        # address in (LOCALHOST, ServerPort) format
        address = server.sockets[0].getsockname()
        print("Server {} starting on {}".format(self.id, address))
        
        # run server until interrupted by ctrl+c
        async with server:
            await server.serve_forever() # built-in, start accepting connections until coroutine is canceld

        # close server
        server.close()
        
        # close server log file


    # Servers can communicate to each ther using AT messages through a flooding algorithm,
    # which propogates location pdates to each other.
    # Servers should not propagate place information to each other, only locations; 
    # when asked for place information, a server should contact Google Places directly for it. 
    # Servers should continue to operate if their neighboring servers go down, that is, drop a connection and then reopen a connection later.

    async def propagateMessage(self, message):
        # for each neighbor of current server self.id, propagate the server message to its neighbors
        for neighbor in networkFlow[self.id]:
            try:
                # open_connection is part of asyncio api
                logging.info("Connecting to {} from {}".format(neighbor, self.id))
                reader, writer = await asyncio.open_connection(LOCALHOST, servers[neighbor])
                logging.info("{} sent to neighbor {} this: {}". format(self.id, neighbor, message, ))
                writer.write(message.encode())
                await writer.drain() # flush write buffer
                
                # end connection
                logging.info("Closing connection to {}\n".format(neighbor))
                writer.close()
                await writer.wait_closed()
            except:
                logging.info("Exception: Unable to connect to server {} from {}\n".format(neighbor, self.id))
            # except IndexError: #sequence index out of range
             #   logging.info('Index error on server')


def main():
    # serverID = sys.argv[1]
    if serverID not in servers:
        print("Invalid server name: {}".format(serverID))
        print("Valid server names: {}".format(list(servers.keys())))
        sys.exit(1)
    
    # create Server object
    server = Server(serverID)
    try:
        asyncio.run(server.runServer())
    except KeyboardInterrupt:
        print("Shutting down server...")
        sys.exit(1)

    """
    # parse command line prompt
    parser = argparse.ArgumentParser()
    parser.add_argument('serverID', type=str, help='Please input server name') #built-in
    args = parser.parse_args # built-in

    # create Server object
    server = Server(args.serverName)
    # Invalid server entered
    if serverID not in servers:
        print("Invalid server name: \"{}\"".format(serverID))
        print("Valid server names: {}".format(list(servers.keys())))
        sys.exit(1)
    
    
    # asyncio.run(server.start(ports[server_name]))
    """

if __name__ == '__main__':
    main()
