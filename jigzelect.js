// TODO: later remove this.children to replace everything by local node-cache storage, but not until object serialization with node-cache does not work appropriately

var util 		= require('util'); 
var EventEmitter 	= require('events').EventEmitter; 
var ZooKeeper 		= require('node-zookeeper-client'); 
var NodeCache 		= require('node-cache'); 

var debug 		= require('debug')('jigzelect'); 

var ZNODE_ELECT = "/jighost_elect";  	// default node if not provided in conf
var ELECT_VERSION = "1";  
var MAX_PER_OBJECT = 5; 
var ZNAME  = "jigcandidate_";

var CACHE_STD_TTL = 0; 
var CACHE_CHECK_PERIOD = 0;  

var KEY_NUM_CURRENT_TENANTS = "_num_current_tenants"; 
var KEY_MAX_TENANTS = "_max_tenants";

var KEY_PREFIX_WATCH = "_watch_";

var Jigzelect = function(conf, max) {

	// conf
	this.conf = conf; 

	// cache
	var ttl = this.conf.cache_ttl || CACHE_STD_TTL ; 
	var period = this.conf.cache_checkperiod || CACHE_CHECK_PERIOD; 
	this.cache = new NodeCache({stdTTL:ttl, checkperiod: period}); 

	this.max_tenants = max; 
	this.cache.set(KEY_MAX_TENANTS, this.max_tenants); 
	this.current_num_tenants = 0;
	this.cache.set(KEY_NUM_CURRENT_TENANTS, this.current_num_tenants);  

	this.children = {};

	// initialize a zookeeper instance
	this.zookeeper = ZooKeeper.createClient(this.conf.zk_uri, { 
		sessionTimeout: this.conf.zk_timeout
		, spinDelay: this.conf.zk_spindelay 
		, retries: this.conf.zk_retries
	});

	this.elect_path = this.conf.zk_path_elect || ZNODE_ELECT ; 

	var self = this; 

	this.zookeeper.once('connected', function(err) {
	    if (err) throw err; 
	    debug("Zk session established"); 

	    // try to get the elect path node, if does'nt exist, we create it
	    var that = self; 
	    self.zookeeper.exists(self.elect_path,
            	function (event) {
			//watcher 
			handleWatchEvent(event); 
            	},
            	function(error, stat) {
               		if (error) {
                       	 	debug(error.stack); 
				return;
			} 
			if (stat) {
			 	debug('ZNode %s exists', that.elect_path); 
				that.emit('connected', that.elect_path); 
			} else {
				debug('ZNode does not exist, create it'); 
				var _that = that; 
			       self.zookeeper.create(that.elect_path, function(error){
                                        if (error) {
                                                debug(error.stack);
                                                _that.emit('disconnected', _that.elect_path); return ;
                                        }
                                        else {
                                                debug("Zk node %s successfully created.", _that.elect_path);
                                                _that.emit('connected', _that.elect_path);
                                        }
                                });
                	}
            	}
	     );
	});

	this.zookeeper.connect(); 
	
	function handleWatchEvent(e) {
		debug("got event= %s", e); 
                if (e.type == ZooKeeper.Event.TYPES.NODE_DELETED) { 
			debug ('received a deleted event %e', e); 
		} else if (e.type == ZooKeeper.Event.TYPES.NODE_CREATED) {
			debug ('received a created event %e', e); 
		} else if (e.type == ZooKeeper.Event.TYPES.CHILDREN_CHANGED) {
			debug('received a children changed event %e', e); 		
		}
	}

	EventEmitter.call(this); 

}

util.inherits(Jigzelect, EventEmitter); 


/*
	key: id of the election object
	zoo_path: path to the election object
	max: number of candidates allowed 
*/


Jigzelect.prototype.addElectObject = function (key, zoo_path, max, cb) {
	var self = this; 
	this.zookeeper.exists(this.elect_path+'/'+key, function(error, stat) {
		if (error) { debug(error.stack) ; return cb(error);  } 
		if (stat) { debug('node exists, nothing to add. Stats=%j',stat); return cb(self.elect_path+'/'+key); } 
		else { // let's create it 
			debug('node does not exist, let s create it') ;
			var data = {id: key, path: zoo_path, max:max | MAX_PER_OBJECT}; 
			var buffer = new Buffer(JSON.stringify(data)); 		
			debug("create node with data = %j", data);
   			// console.log("buffer to json = "); var str = buffer.toString('utf-8'); console.log(JSON.parse(str)); 

			self.zookeeper.create(self.elect_path+'/'+key, buffer, function(error, path){
				if (error) { console.log(error.stack); return cb(error) ; } 
				debug('Znode: %s is created for candidate election.', path);
				return cb(null, path);  
			});  

		} 
	});  
};

Jigzelect.prototype.getElectObject = function (key, cb) {
	var self = this; 
	this.zookeeper.getData(this.elect_path+'/'+key,
		null, 
		function(error, data, stat) {
                        if (error) {
                                debug(error.stack); cb(error);
                        } else {
                                debug("number of children :"+ stat.numChildren);
				var jdata = JSON.parse(data.toString('utf-8')); 
				debug("got elect object : %j", jdata); 
				return cb(null, jdata); 
	 		}
		}
	);
}

Jigzelect.prototype.delElectObject = function (key, cb) {
	var self = this; 
	this.zookeeper.remove(this.elect_path+'/'+key, -1, function (error) {
		if (error) { debug (error.stack); return cb(error) ; }
		debug ('Znode: %s is deleted and no more available for candidate election.', self.elect_path+'/'+key); 
		cb(null, self.elect_path+'/'+key); 
	}); 
};

Jigzelect.prototype.getElectObjects = function (cb) {
	var self = this; 
	// list and watch childrens
	this.zookeeper.getChildren( this.elect_path, 
		null , 
		function (error, children, stat) { 
			if (error) { debug('failed to list children of %s due to: %s', self.elect_path, error); return; } 
			debug ("children of %s are: %j", self.elect_path, children); 
			cb (null, children); 
		}
	); 
};


Jigzelect.prototype.anyCandidate = function () {

        var self = this;
        // list and watch childrens
        this.zookeeper.getChildren( this.elect_path,
                function(event){
			debug('got watcher event: %s', event); 

			debug('event type');debug(event.type); 
			if (event.type == ZooKeeper.Event.NODE_DELETED) {
                       		debug ('received a deleted event %e', event);
                	} else if (event.type == ZooKeeper.Event.NODE_CREATED) {
                        	debug ('received a created event %e', event);
                	} else if (event.type == ZooKeeper.Event.CHILDREN_CHANGED) {
                        	debug('received a children changed event %e', event);
                	} else {

			}


			if (self.current_num_tenants < self.max_tenants) { // we can candidate to host	
				debug('candidate again');  
				self.anyCandidate(); 
			}
		},
                function (error, children, stat) {
                        if (error) { debug('failed to list children of %s due to: %s', self.elect_path, error); return; }
                        debug ("children of %s are: %j", self.elect_path, children);

			// A. If no children known, try to candidate
			// 	FFS: restrict number of allowed candidates per child?
				for (var i =0; i<children.length; i++) {
					var child = children[i];
					self.candidate(child); 
				}
                        // if previously handled jigzcontext is no more, we shall remove it and notify 
			//children.forEach(function(child){
			//	self.children = children;
			//}); 
                }
        );

	/*
	function isElectable(scope, child, cb) {
	    var that = scope;  
	    scope.zookeeper.getData(scope.elect_path,
                null,
                function(error, data, stat) {
                        if (error) {
                                console.log(error.stack); cb(error);
                        } else {
                                console.log("isElectable::get data from zk node: %j", data.toJSON());
				console.log("number of children :"+ stat.numChildren);
				var jdata = data.toJSON(); 
				console.log("max allowed : "+jdata.max);
				if (jdata.max){ 
					if (jdata.max < numChildren) { return cb (null, false) ;}
					else { return cb(null, true); }  
				}
                        }
            });
	}
	*/
	
};

Jigzelect.prototype.candidate = function(key) {
        var self = this; 

	// check ... if we already candidated for this key, no need to candidate again 
	var isc = false;
	
	// have some issues with object serialization in cache 
	//var c = this.cache.get(key); 
	//debug("retrieved cache for key:%s =>  value = %j", key, c); 
	//if (c){ isc = c.candidated; }
	
	if (this.children.hasOwnProperty(key)) 
		var isc = this.children[key].candidated; 

	// if max number of tenants reached, no need to candidate again 
	if (self.getNumberOfLeads() >= self.max_tenants) { // we can candidate to host
            isc = true; 
        }

	if (isc == true) { debug("ALREADY CANDIDATED or max tenants reached..."); return ;} 
	else { 
   	     debug("allowed to candidate ... continue ");  
	     // need to position candidated to true beforehand to avoid double candidate calls
	     this.children[key] = {candidated: true, leader:false}
   	     this.zookeeper.create(self.elect_path+'/'+key+'/'+ZNAME, ZNAME, ZooKeeper.CreateMode.EPHEMERAL_SEQUENTIAL, 
	            function(error, path){
			if (error) { debug(error.stack); return ; } 
			//setting watcher
			var that = self; 
			var mypath = path; 
			// updating candidate path 
			var obj = {candidated: true, leader: false, path: mypath}; 
			self.children[key] = obj ; // means we candidated, and we know the path

			self.cache.set(key, self.children[key]); 
 	
			self.assessLeader(key, mypath); 

	     	}
	    );
	}
};

Jigzelect.prototype.assessLeader = function (key, mypath) {

	var that = this; 

	this.zookeeper.getChildren(this.elect_path+'/'+key,
        	null,
                function (error, children, stat){
 	               if (error) { debug(error.stack); return; }
                       debug("got ephemeral children : %j", children)  ; 
                       // shall watch now...
                       debug("my path to compare = %s", mypath);

                       var parray = mypath.split('_');
                       var mynum = Number(parray[parray.length-1]);
		       var pos = getNextLeadPosition(mynum, children);
			
//                       if (mypath.indexOf(children[0]) > -1) {
		       if (pos == -1) {  
                  	   debug("we are the leader...");
			   var c = that.cache.get(key); 
			   var isnewlead = false; 
			   if (c) {
				if (c.leader == true && c.path == mypath) debug('we are already the leader'); 
				else isnewlead = true; 
			   } else { isnewlead = true ; }
			   
			   if (isnewlead) { 
	                           that.current_num_tenants+=1;
        	                   that.children[key].leader = true;
	
        	                   that.cache.set(KEY_NUM_CURRENT_TENANTS, that.current_num_tenants);  
                	           that.cache.set(key, that.children[key]);
                        	   that.emit('leader-granted', {key: key, path: mypath});
			   }

                           // WATCH MYSELF 
                           var _that = that;
			   debug("watching ourself with path=%s", mypath); 
                           that.zookeeper.exists(mypath, 
			       function(event){
                  	   	   debug ('received event type: %s for path: %s', event.type, event.path);
                                   if (event.type == ZooKeeper.Event.NODE_DELETED) {
					//var child2 = JSON.parse(JSON.stringify(_that.cache.get(key))); 
					//debug("GOT FROM CACHE: %j", child2); 
					var child = _that.children[key]; 
                                        if (child) {
                 	                      if (child['leader'] == true && child['path'] == event.path){
						    debug("RELEASED LEADER "); 
						    if (_that.current_num_tenants > 0) _that.current_num_tenants -= 1; 
						    _that.children[key]['leader'] = false; 
						    _that.children[key]['path'] = null; 
						    _that.children[key]['candidated'] = false;  
						    _that.cache.set(key, that.children[key]); 
                                                    _that.emit('leader-released', {key: key, path: event.path});
		   				    // candidate again
	  	                                    _that.candidate(key);
					      }
   					}
                                   } else if (event.type == ZooKeeper.Event.CHILDREN_CHANGED) {
             	                         debug('received a children changed event %e', event);
                                      // shouldn't be relevant here in ephemeral / sequence pattern
                                   } else {
                                        debug('doing nothing...');
                                   }
                               }, 
			       function (error, stat) { 
				    if (error) { debug(error.stack); return }
				    if (stat) { debug('node exists: %j', stat); } 
				    else { debug("can t watch ourself, node does not exist..."); } 
			       }	
			    );
                        } else {
                        	// WATCH FOR NEXT LEADER IF ANY
				debug("add watcher on next leader at pos %s", pos); 
                                var _that = that;
                                debug("I am candidate number :: %s , and I watch child :: %s", mynum, children[pos]);
                                //that.cache.set(KEY_PREFIX_WATCH+key+'_'+children[postowatch], true);
                                that.zookeeper.exists(that.elect_path+'/'+key+'/'+children[pos],
                                    function(event){
                               		debug ('received event type: %s for path: %s', event.type, event.path);
                                        if (event.type == ZooKeeper.Event.NODE_DELETED) {
                                             // analyze again...
                                             _that.assessLeader(key, mypath);
                                        }
                                    },
                                    function(error, stat){
                                        if (error) { debug(error.stack); return; }
                                        if (stat) { debug('found child to watch => %j', stat); return; }
                                        else { debug('child does not exist'); return ; }
                                    }
                                );
			}

		}
       );

	// return -1 if no next leader position, otherwise return the position of the next leader 
	function getNextLeadPosition(current, elems){
                var mynum = current; 
                var postowatch=-1;
                // identify next watcher (we'll optimize later) 
                var tnum = -1;
                for (var p=1; p < elems.length; p++) {
                	var checkarray = elems[p].split('_'); // we extract the sequence number 
                        var checknum = Number(checkarray[checkarray.length-1]);
                        if (checknum < mynum && checknum > tnum ) { tnum = checknum; postowatch = p ; }
                }
		debug ('found next sequence number is %s', tnum); 
		return postowatch; 
	}

}

/*
	Revoke an existing candidate
*/
Jigzelect.prototype.revokeCandidate = function (key, cb) {
	if (this.children.hasOwnProperty(key)) {
		var self = this; 
		var path = this.children[key].path; 
		debug('removing znode at path=%s', path); 
		this.zookeeper.remove(path, function(error){
			if (error) { debug(error.stack); return cb(error); }
			if (self.children[key].leader == true) {
				if(self.current_num_tenants > 0) self.current_num_tenants-=1; 
			        //self.children[key].leader = false; 
				self.cache.set(KEY_NUM_CURRENT_TENANTS, self.current_num_tenants); 
			} 
			delete(self.children[key]); 
			self.cache.del(key); 
			//self.children[key] = undefined; 
			debug('Znode %s deleted.', path);  
                        cb(null); 
		}.bind(this)); 		
	} else {
		cb('error, bad key: %s', key); 
	}
};


/* purge data ... use with caution */
Jigzelect.prototype.purge = function (key, cb) {
	this.cache.flushAll(); 	
	this.current_num_tenants = 0; 
	this.children = {}; 
}

/*
	Set the maximum number of tenants the libraries will be leading
*/
Jigzelect.prototype.setMaxTenants = function (max) {
	this.max_tenants = max; 
};

/*
	return an Array list of objects for which we are the leader  
*/
Jigzelect.prototype.getMyLeaders = function(cb){
	var leaders = [];
	var keys = Object.keys(this.children); 
	for (var i=0; i<keys.length; i++){
		if (this.children[keys[i]].leader == true) {
			leaders.push(keys[i]); 
		}
	} 
	return cb(null, leaders); 
}

/*
constraints: sync (i.e. return without cb) may not return the exact number. But normally the decay shall not happen, that s just a additional security 
*/
Jigzelect.prototype.getNumberOfLeads = function(cb){

	var num = this.current_num_tenants;

	// realign in case of decays 
	this.getMyLeaders(function(err, leads){
		if (err) return; 
		var effective = leads.length;  
		if (num != effective) num = effective; 
		if (cb) return cb(null, num); 
	}); 

	if (!cb) return num;  
}

module.exports = Jigzelect; 

