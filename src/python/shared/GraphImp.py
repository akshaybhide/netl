#!/usr/bin/python

import re, os, sys, fileinput
import CompJava

class GraphImp:
	
	_edgeMap = {}
	
	def addNode(self, nodeid):

		self._edgeMap.setdefault(nodeid, set())
		
	def getNodeList(self):
		return [onenode for onenode in self._edgeMap]

	def edgeSet4Node(self, nodesrc):
		return self._edgeMap[nodesrc]

	def addEdge(self, nodesrc, nodedst):
		
		assert nodesrc in self._edgeMap, "Node %s not found in edgemap" % (nodesrc)
		assert nodedst in self._edgeMap, "Node %s not found in edgemap" % (nodedst)
		
		self._edgeMap[nodesrc].add(nodedst)


	def findCycle(self, srcnode):
		
		pathmap = {}
		path2trg = [srcnode]
		
		for nbor in self._edgeMap[srcnode]:
			if not nbor in pathmap:
				self._popPathMapSub(nbor, path2trg, pathmap)
		
		return pathmap[srcnode]

	def _popPathMapSub(self, trgnode, path2trg, pathmap):

		#print "Calling for trgnode=%s" % (trgnode)

		assert not trgnode in pathmap, "Pathmap already includes node %s" % (trgnode)
		
		#print "Pathmap keys are %s" % (",".join([onekey for onekey in pathmap]))
		
		# add path2trg to pathmap
		sublist = []
		sublist.extend(path2trg)
		sublist.append(trgnode)
		pathmap[trgnode] = sublist
		
		for nbor in self._edgeMap[trgnode]:
			if not nbor in pathmap:
				newp2t = []
				newp2t.extend(path2trg)
				newp2t.append(trgnode)
				#print "Going to call for nbor=%s" % (nbor)
				self._popPathMapSub(nbor, newp2t, pathmap)


	def reachableSet(self, nodesrc, addset):
		
		for reachme in self._edgeMap[nodesrc]:
			if not reachme in addset:
				addset.add(reachme)
				self.reachableSet(reachme, addset)
			
	def cycleFromNode(self, nodesrc):
		rset = set()
		self.reachableSet(nodesrc, rset)
		return nodesrc in rset
		
			
	def hasCycle(self):
		
		for srcnode in self._edgeMap:
			rset = set()
			self.reachableSet(srcnode, rset)
			
			if srcnode in rset:
				return True
				
		return False
		

if __name__ == "__main__":


	mygraph = GraphImp()
	
	mygraph.addNode('danb')
	mygraph.addNode('hlb')
	mygraph.addNode('nextnode')
	
	mygraph.addEdge('danb', 'hlb')
	mygraph.addEdge('hlb', 'nextnode')
	mygraph.addEdge('nextnode', 'danb')
	
	mycycle = mygraph.findCycle('danb')
	
	print "Found cycle: %s" % (mycycle)
	

