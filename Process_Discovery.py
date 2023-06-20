
from datetime import datetime, timedelta
import graphviz as pgv
from collections import defaultdict
import math

class Lossy_count:
    def __init__(self, max_approx_error = 0.1):
        self.__events = dict() #map where key is an event and the values are (frequency, bucket)
        self.__observed_events = 1 #number of observed events
        self.__bucket_width = int(math.ceil(1/max_approx_error)) #bucket width
        self.__printRefreshRate = self.__bucket_width #default refreshrate

    def __update(self, newEvent):
        current_bucket = int(math.ceil(self.__observed_events / self.__bucket_width)) #calculated current bucket

        if newEvent.get_event_name() in self.__events: #if event exist
            lastEvent = self.__events[newEvent.get_event_name()] #save former event locally
            del self.__events[newEvent.get_event_name()] #increment event frequency
            self.__events[newEvent.get_event_name()] = (lastEvent[0] + 1, lastEvent[1])

        else:
            self.__events[newEvent.get_event_name()] = (1, current_bucket - 1) #add the event

        if self.__observed_events % self.__bucket_width == 0.0: #cleanup time based on max aproximation error

            tobedel = []  #list of events to be deleted

            for event, (frequency, bucket) in self.__events.items(): #finds all keys to be removed
                if(frequency+bucket <= current_bucket):
                    tobedel.append(event)
            for eventName in tobedel: #cant be deleted on previous for loop as it moves it around
                del self.__events[eventName]


        self.__observed_events += 1


    def __print_data(self): #print all events currently saved
        if (self.__observed_events - 1) % self.__printRefreshRate == 0:
            print("New print")
            for eventName, (frequency, bucket) in self.__events.items():
                print("Event: {0}, discovered in bucket: {1}, and has occured: {2} times".format(eventName,bucket,frequency))


    def subscribe(self, source): #subscribes the source to the class
        source.subscribe(
            on_next=lambda i:{
                self.__update(i),
                self.__print_data()
            },
            on_error=lambda e, l: print("Error Occurred: {0} to {1}".format(e, l)),
        )

    def setMax_approx_error(self, max_approx_error): #changes bucket width
        self.__bucket_width = int(math.ceil(1/max_approx_error))

    def setPrintRefreshRate(self, rate): #set how often to print the event set
        self.__printRefreshRate = rate

    def getEvents(self): #returns the map of events
        return self.__events

    def getEventsObserved(self): #return amount of events occured
        return self.__observed_events - 1

class HM_LC:
    def __init__(self, max_approx_error = 0.1 , dependency_threshold = 0, and_threshold = 0.8):
        self.__minimum_dependency_threshold = dependency_threshold #set dependency threshold to be added to the model
        self.__and_threshold = and_threshold #set the "and" threshold for when 2 edges leave a node on model


        self.__D_C = dict() #set of event
        self.__D_R = dict() #set of relations
        self.__observed_events = 1
        self.__bucket_width = int(math.ceil(1 / max_approx_error)) #set bucket width

        self.__modelRefreshRate = self.__bucket_width #default model refreshrate
        self.__modelNumber = 1 #used to keep track of number of models completed
        self.__modelName = 'HM_LC' #set default file name on the models

        self.__labelType = 1 #set default label information to be frequency
        self.__rounderforLabel = 2 #set default number of rounding on label information

    def __update_HM_LC(self, newEvent):
        current_bucket = int(math.ceil(self.__observed_events / self.__bucket_width)) #calculated bucket
        if newEvent.get_trace_name() in self.__D_C: #if caseID already exist
            lastEvent = self.__D_C[newEvent.get_trace_name()] #localy save former event

            del self.__D_C[newEvent.get_trace_name()] #replace caseID's former event with new event
            self.__D_C[newEvent.get_trace_name()] = [newEvent.get_event_name(), lastEvent[1] + 1, lastEvent[2],
                                                   newEvent.get_event_time()]

            r_N = (lastEvent[0], newEvent.get_event_name()) #save relation localy

            if r_N in self.__D_R: #if relation exists in set
                lastRelation = self.__D_R[r_N]  #localy save former relation
                del self.__D_R[r_N] #replace relation

                diff = (newEvent.get_event_time() - lastEvent[3]) - lastRelation[2] #calculates the new average relation time
                newTime = lastRelation[2] + (diff/(lastRelation[0] + 1))

                self.__D_R[r_N] = [lastRelation[0] + 1, lastRelation[1], newTime]
            else: #the relation doesent exist, create it
                self.__D_R[r_N] = (1, current_bucket - 1, newEvent.get_event_time() - lastEvent[3])

        else: #caseID doesnt exist, create it
            self.__D_C[newEvent.get_trace_name()] = (newEvent.get_event_name(), 1, current_bucket - 1, newEvent.get_event_time())

        #clean up
        D_Ctobedel = [] #the 2 lists are needed to avoid messing with the comming loops
        D_Rtobedel = []

        #bucket cleaning time
        if self.__observed_events % self.__bucket_width == 0.0:

            for caseID, (eventName, frequency, bucket, time) in self.__D_C.items():
                if frequency + bucket <= current_bucket: #if not above the error threshold on all events occured
                    D_Ctobedel.append(caseID)

            for caseID in D_Ctobedel: # deleted the event
                del self.__D_C[caseID]

            for relation,(frequency, bucket, time) in self.__D_R.items():
                if frequency + bucket <= current_bucket: #if not above the error threshold of all relations
                    D_Rtobedel.append(relation)

            for relation in D_Rtobedel: #delete the relations
                del self.__D_R[relation]

        self.__observed_events += 1

    def __update_model(self):
        if (self.__observed_events - 1) % self.__modelRefreshRate == 0: #model refreshrate user defined
            G = pgv.Graph(comment='HM_LC') #setting type of graph
            G.graph_attr['rankdir'] = 'LR'
            G.node_attr['shape'] = 'box'
            G.edge_attr['arrowType'] = 'normal'
            above_dependency_threshold = defaultdict(list)


            for (A,B),(frequency, bucket, time)  in self.__D_R.items(): #for all relations and all their attributes
                dependency = 0
                if (B,A) in self.__D_R: #calculated dependecency of each relation
                    op_frequency = self.__D_R[(B,A)][0]
                    dependency = (frequency - op_frequency) / (frequency + op_frequency + 1)
                else:
                    dependency = frequency / (frequency + 1)
                if dependency >= self.__minimum_dependency_threshold: #if within dependency relation
                    above_dependency_threshold[A].append((B, round(frequency,self.__rounderforLabel),round(dependency,self.__rounderforLabel), time))

            #for the next line of code, A will be the variable for where the edge is coming from
            #C and B will be the where the edge is going to, and its the and/XOR relation for C and B
            for (A), (listOfB) in above_dependency_threshold.items(): #everything above dependency threshold
                choices = len(listOfB) # all that A goes to
                if choices != 2: # if A goes to only 1 or more than to, have to visual idea to implement for multiple paths
                    for i in range(0, choices):  # loop over all listofB
                        G.edge(A, listOfB[i][0], label=str(listOfB[i][self.__labelType]), dir='forward')
                else:
                    and_relation = False
                    for i in range(0,choices): # for all B's that A goes to
                        for j in range(i + 1, choices): # for all C's that B can have a relation with
                            #frequency for each that goes to each
                            B_to_C = 0
                            C_to_B = 0
                            A_to_B = listOfB[i][1]
                            A_to_C = listOfB[j][1]

                            B = listOfB[i][0]
                            C = listOfB[j][0]

                            #if B and C mutual goes to each other
                            if (B,C) in self.__D_R:
                                B_to_C = self.__D_R[(B,C)][0]
                            if (C,B) in self.__D_R:
                                C_to_B = self.__D_R[(C,B)][0]

                            #and relation formular
                            and_relation_Num = 0
                            and_relation_Num = (B_to_C + C_to_B) / (A_to_B + A_to_C + 1)

                            if(and_relation_Num >= self.__and_threshold): #if above threshold
                                and_relation = True

                        if(and_relation): #put and on the label
                            G.edge(A, listOfB[i][0], label=str(listOfB[i][self.__labelType]) +" AND", dir='forward')
                        else: # put XOR on the label
                            G.edge(A, listOfB[i][0], label=str(listOfB[i][self.__labelType]) + " XOR", dir='forward')


            G.render('model-output/' + self.__modelName + str(self.__modelNumber) + '.gv').replace('\\', '/')
            self.__modelNumber += 1



    def subscribe(self, source): #subscribes source to the miner
        source.subscribe(
            on_next=lambda i: #for each new event i
            {
                self.__update_HM_LC(i), #update HM_LC with the new event
                self.__update_model() #update the painted model
            },
            on_error=lambda e, l: print("Error Occurred: {0} to {1}".format(e, l)),
        )

    def setMax_approx_error(self, max_approx_error): #set the new bucker width
        self.__bucket_width = int(math.ceil(1 / max_approx_error))

    def setMin_dependency_threshold(self, dependency_threshold): #set new min depenedency threshold
        self.__minimum_dependency_threshold = dependency_threshold

    def setAnd_threshold(self, and_threshold): #set new and threshold
        self.__and_threshold = and_threshold

    def setModelRefreshRate(self, refreshRate): #set how many events should occur before making a new model
        if refreshRate >= 1:
            self.__modelRefreshRate = int(refreshRate)


    def setLabel(self, label): #set the type of information each edge on the model should have
        match label:
            case "Time": #the average time for each relation
                self.__labelType = 3
                return
            case "Dependency": #the dependecy for each relation
                self.__labelType = 2
                return
            case "Frequency": #the amount of times the relation has occured
                self.__labelType = 1
                return

    def setlabelRounder(self, rounder): #set how many decimals behind numbers to occur on labels
        if rounder >= 0:
            self.__rounderforLabel = int(rounder)

    def setFileName(self, name): #set the name of the files to be saved
        self.__modelName = name

    def getD_C(self): #return map of case ids
        return self.__D_C

    def getD_R(self): #return to map of relations
        return self.__D_R

    def getEventsObserved(self): #return the total number of events observed
        return self.__observed_events - 1


class HM_LCB:
    def __init__(self, budget = 10 , dependency_threshold = 0, and_threshold = 0.8):
        self.__budget = int(budget) #max length of stored events and relations
        self.__minimum_dependency_threshold = dependency_threshold
        self.__and_threshold = and_threshold


        self.__D_C = dict() #set of event
        self.__D_R = dict() #set of relations
        self.__observed_events = 1
        self.__current_bucket = 0

        self.__modelRefreshRate = 1
        self.__modelNumber = 1
        self.__modelName = 'HM_LCB'

        self.__labelType = 1
        self.__rounderforLabel = 2


    def __update_HM_LCB(self, newEvent):

        #for budget lossy counting, if relation or caseID already exists, the memory will just be replaced and not expanded
        #so we only clean up when the caseID or relation doesnt exist, which results in creating a new one
        if newEvent.get_trace_name() in self.__D_C: #if caseID already exist
            lastEvent = self.__D_C[newEvent.get_trace_name()] #localy save former event
            del self.__D_C[newEvent.get_trace_name()] #replace caseID's former event with new event
            self.__D_C[newEvent.get_trace_name()] = [newEvent.get_event_name(), lastEvent[1] + 1, lastEvent[2],
                                                   newEvent.get_event_time()]


            r_N = (lastEvent[0], newEvent.get_event_name()) #save relation localy

            if r_N in self.__D_R: #if relation exists in set
                lastRelation = self.__D_R[r_N]  #localy save former relation
                del self.__D_R[r_N] #replace relation

                diff = (newEvent.get_event_time() - lastEvent[3]) - lastRelation[2] #incase the user wants the time for each relation
                newTime = lastRelation[2] + (diff/(lastRelation[0] + 1))

                self.__D_R[r_N] = [lastRelation[0] + 1, lastRelation[1], newTime]

            else: #the relation doesent exist, create it
                while len(self.__D_R) + len(self.__D_C) >= self.__budget:  # if budget is reached when adding a new key + iten
                    self.__bucket_cleaning()  # bucket cleaning time
                self.__D_R[r_N] = (1, self.__current_bucket, newEvent.get_event_time() - lastEvent[3])

           # print("from {0} to {1} ocured {2} times at a time of {3}".format(r_N[0],r_N[1],self.__D_R[r_N][0],self.__D_R[r_N][2]))

        else: #caseID doesnt exist, create it
            while ((len(self.__D_R) + len(self.__D_C)) >= self.__budget): #if budget is reached when adding a new key + iten
                self.__bucket_cleaning() # bucket cleaning time
            self.__D_C[newEvent.get_trace_name()] = (newEvent.get_event_name(), 1, self.__current_bucket, newEvent.get_event_time())


        #clean up
        self.__observed_events += 1

    def __bucket_cleaning(self):
        self.__current_bucket += 1 #increment bucket to clean all items not within the new bucket number

        D_Ctobedel = [] #the 2 lists are needed to avoid messing with the comming loops
        D_Rtobedel = []

        for caseID, (eventName, frequency, bucket, time) in self.__D_C.items(): #for all caseIDs' occured
            if frequency + bucket <= self.__current_bucket:  # if not above the bucket threshold on all events occured
                D_Ctobedel.append(caseID)

        for caseID in D_Ctobedel:  # deleted the event
            del self.__D_C[caseID]

        for relation, (frequency, bucket, time) in self.__D_R.items(): #for all relations occured
            if frequency + bucket <= self.__current_bucket:  # if not above the bucket threshold of all relations
                D_Rtobedel.append(relation)

        for relation in D_Rtobedel:  # delete the relations
            del self.__D_R[relation]

    def __update_model(self):
        if (self.__observed_events - 1) % self.__modelRefreshRate == 0: #model refreshrate user defined
            G = pgv.Graph(comment='HM_LC') #setting type of graph
            G.graph_attr['rankdir'] = 'LR'
            G.node_attr['shape'] = 'box'
            G.edge_attr['arrowType'] = 'normal'
            above_dependency_threshold = defaultdict(list)


            for (A,B),(frequency, bucket, time)  in self.__D_R.items(): #for all relations and all their attributes
                dependency = 0
                if (B,A) in self.__D_R: #calculated dependecency of each relation
                    op_frequency = self.__D_R[(B,A)][0]
                    dependency = (frequency - op_frequency) / (frequency + op_frequency + 1)
                else:
                    dependency = frequency / (frequency + 1)
                if dependency >= self.__minimum_dependency_threshold: #if within dependency relation
                    above_dependency_threshold[A].append((B, round(frequency,self.__rounderforLabel),round(dependency,self.__rounderforLabel), time))

            #for the next line of code, A will be the variable for where the edge is coming from
            #C and B will be the where the edge is going to, and its the and/XOR relation for C and B
            for (A), (listOfB) in above_dependency_threshold.items(): #everything above dependency threshold

                choices = len(listOfB) # all that A goes to
                if choices != 2: # if A goes to only 1 or more than to, have to visual idea to implement for multiple paths
                    for i in range (0, choices): #loop over all listofB
                        G.edge(A, listOfB[i][0], label=str(listOfB[i][self.__labelType]), dir='forward')
                else:
                    and_relation = False
                    for i in range(0,choices): # for all B's that A goes to
                        for j in range(i + 1, choices): # for all C's that B can have a relation with
                            #frequency for each that goes to each
                            B_to_C = 0
                            C_to_B = 0
                            A_to_B = listOfB[i][1]
                            A_to_C = listOfB[j][1]

                            B = listOfB[i][0]
                            C = listOfB[j][0]

                            #if B and C mutual goes to each other
                            if (B,C) in self.__D_R:
                                B_to_C = self.__D_R[(B,C)][0]
                            if (C,B) in self.__D_R:
                                C_to_B = self.__D_R[(C,B)][0]

                            #and relation formular
                            and_relation_Num = 0
                            and_relation_Num = (B_to_C + C_to_B) / (A_to_B + A_to_C + 1)

                            if(and_relation_Num >= self.__and_threshold): #if above threshold
                                and_relation = True

                        if(and_relation): #put and on the label
                            G.edge(A, listOfB[i][0], label=str(listOfB[i][self.__labelType]) +" AND", dir='forward')
                        else: # put XOR on the label
                            G.edge(A, listOfB[i][0], label=str(listOfB[i][self.__labelType]) + " XOR", dir='forward')


            G.render('model-output/' + self.__modelName + str(self.__modelNumber) + '.gv').replace('\\', '/')
            self.__modelNumber += 1

    def subscribe(self, source): #subscribes source to the miner
        source.subscribe(
            on_next=lambda i: #for each new event i
            {
                self.__update_HM_LCB(i), #update HM_LCB with the new event
                self.__update_model() #update the painted model
            },
            on_error=lambda e, l: print("Error Occurred: {0} to {1}".format(e, l)),
        )

    def setBudget(self, newBudget): #set new budget
        self.__budget = int(newBudget)

    def setMin_dependency_threshold(self, dependency_threshold): #set new min dependency threshold
        self.__minimum_dependency_threshold = dependency_threshold

    def setAnd_threshold(self, and_threshold) :#set new and threshold
        self.__and_threshold = and_threshold

    def setModelRefreshRate(self, refreshRate): #set how many events should occur before making a new model
        if refreshRate >= 1:
            self.__modelRefreshRate = int(refreshRate)

    def setLabel(self, label): #set the type of information each edge on the model should have
        match label:
            case "Time": #the average time for each relation
                self.__labelType = 3
                return
            case "Dependency": #the dependecy for each relation
                self.__labelType = 2
                return
            case "Frequency": #the amount of times the relation has occured
                self.__labelType = 1
                return

    def setlabelRounder(self, rounder): #set how many decimals behind numbers to occur on labels
        if rounder >= 0:
            self.__rounderforLabel = int(rounder)

    def setFileName(self, name): #set the name of the files to be saved
        self.__modelName = name

    def getD_C(self): #returns map of case ids
        return self.__D_C

    def getD_R(self): #returns map of relations
        return self.__D_R

    def getEventsObserved(self): #returns map of events observed
        return self.__observed_events - 1