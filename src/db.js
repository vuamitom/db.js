(function ( window , undefined ) {
    'use strict';
    //==== tam - helper ===================
    
    
    var defaultCompare = function (a, b) {
        return a - b; 
    }
    
    Array.prototype.insertIncrement = function (val, comparer) {
        comparer = comparer || defaultCompare;
        for (var i = 0 ; i < this.length ; i++) {
            var other = this[i];
            if (comparer(val, other) < 0) {
                this.splice(i, 0, val);
                return;
            }
        }
        this.push(val);
    }
    //======== end helper ====================
    var indexedDB = window.indexedDB || window.webkitIndexedDB || window.mozIndexedDB || window.oIndexedDB || window.msIndexedDB,
        IDBKeyRange = window.IDBKeyRange || window.webkitIDBKeyRange,
        transactionModes = {
            readonly: 'readonly',
            readwrite: 'readwrite'
        };
        
    var hasOwn = Object.prototype.hasOwnProperty;

    if ( !indexedDB ) {
        throw 'IndexedDB required';
    }

    var defaultMapper = function (value) {
        return value;
    };

    var CallbackList = function () {
        var state,
            list = [];

        var exec = function ( context , args ) {
            if ( list ) {
                args = args || [];
                state = state || [ context , args ];

                for ( var i = 0 , il = list.length ; i < il ; i++ ) {
                    list[ i ].apply( state[ 0 ] , state[ 1 ] );
                }

                list = [];
            }
        };

        this.add = function () {
            for ( var i = 0 , il = arguments.length ; i < il ; i ++ ) {
                list.push( arguments[ i ] );
            }

            if ( state ) {
                exec();
            }

            return this;
        };

        this.execute = function () {
            exec( this , arguments );
            return this;
        };
    };

    var Deferred = function ( func ) {
        var state = 'progress',
            actions = [
                [ 'resolve' , 'done' , new CallbackList() , 'resolved' ],
                [ 'reject' , 'fail' , new CallbackList() , 'rejected' ],
                [ 'notify' , 'progress' , new CallbackList() ],
            ],
            deferred = {},
            promise = {
                state: function () {
                    return state;
                },
                then: function ( /* doneHandler , failedHandler , progressHandler */ ) {
                    var handlers = arguments;

                    return Deferred(function ( newDefer ) {
                        actions.forEach(function ( action , i ) {
                            var handler = handlers[ i ];

                            deferred[ action[ 1 ] ]( typeof handler === 'function' ?
                                function () {
                                    var returned = handler.apply( this , arguments );

                                    if ( returned && typeof returned.promise === 'function' ) {
                                        returned.promise()
                                            .done( newDefer.resolve )
                                            .fail( newDefer.reject )
                                            .progress( newDefer.notify );
                                    }
                                } : newDefer[ action[ 0 ] ]
                            );
                        });
                    }).promise();
                },
                promise: function ( obj ) {
                    if ( obj ) {
                        Object.keys( promise )
                            .forEach(function ( key ) {
                                obj[ key ] = promise[ key ];
                            });

                        return obj;
                    }
                    return promise;
                }
            };

        actions.forEach(function ( action , i ) {
            var list = action[ 2 ],
                actionState = action[ 3 ];

            promise[ action[ 1 ] ] = list.add;

            if ( actionState ) {
                list.add(function () {
                    state = actionState;
                });
            }

            deferred[ action[ 0 ] ] = list.execute;
        });

        promise.promise( deferred );

        if ( func ) {
            func.call( deferred , deferred );
        }

        return deferred;
    };

    var Server = function ( db , name ) {
        var that = this,
            closed = false;

        this.add = function( table ) {
            if ( closed ) {
                throw 'Database has been closed';
            }

            var records = [];
			var counter = 0;
            for (var i = 0; i < arguments.length - 1; i++) {
                if (Array.isArray(arguments[i + 1])) {
                    for (var j = 0; j < (arguments[i + 1]).length; j++) {
                        records[counter] = (arguments[i + 1])[j];
                        counter++;
                    }
                } else {
                    records[counter] = arguments[i + 1];
                    counter++;
                }
            }

            var transaction = db.transaction( table , transactionModes.readwrite ),
                store = transaction.objectStore( table ),
                deferred = Deferred();
            
            records.forEach( function ( record ) {
                var req;
                if ( record.item && record.key ) {
                    var key = record.key;
                    record = record.item;
                    req = store.add( record , key );
                } else {
                    req = store.add( record );
                }

                req.onsuccess = function ( e ) {
                    var target = e.target;
                    var keyPath = target.source.keyPath;
                    if ( keyPath === null ) {
                        keyPath = '__id__';
                    }
                    Object.defineProperty( record , keyPath , {
                        value: target.result,
                        enumerable: true
                    });
                    deferred.notify();
                };
            } );
            
            transaction.oncomplete = function () {
                deferred.resolve( records , that );
            };
            transaction.onerror = function ( e ) {
                deferred.reject( records , e );
            };
            transaction.onabort = function ( e ) {
                deferred.reject( records , e );
            };
            return deferred.promise();
        };

        this.update = function( table ) {
            if ( closed ) {
                throw 'Database has been closed';
            }

            var records = [];
            for ( var i = 0 ; i < arguments.length - 1 ; i++ ) {
                records[ i ] = arguments[ i + 1 ];
            }

            var transaction = db.transaction( table , transactionModes.readwrite ),
                store = transaction.objectStore( table ),
                keyPath = store.keyPath,
                deferred = Deferred();

            records.forEach( function ( record ) {
                var req;
                if ( record.item && record.key ) {
                    var key = record.key;
                    record = record.item;
                    req = store.put( record , key );
                } else {
                    req = store.put( record );
                }

                req.onsuccess = function ( e ) {
                    deferred.notify();
                };
            } );
            
            transaction.oncomplete = function () {
                deferred.resolve( records , that );
            };
            transaction.onerror = function (e) {
                console.error(e);
                deferred.reject( records , e );
            };
            transaction.onabort = function ( e ) {
                deferred.reject( records , e );
            };
            return deferred.promise();
        };
        
        this.remove = function ( table  ) {
            if ( closed ) {
                throw 'Database has been closed';
            }

            var keys = [];
            for (var i = 0; i < arguments.length - 1; i++) {
                keys[i] = arguments[i + 1];
            }


            var transaction = db.transaction( table , transactionModes.readwrite ),
                store = transaction.objectStore( table ),
                deferred = Deferred();
            //Tam: enable batch delete
            keys.forEach(function (key) {
                var req = store.delete(key);
            })
            
            transaction.oncomplete = function () {                
                deferred.resolve( keys );
            };
            transaction.onerror = function (e) {
                console.error(e);
                deferred.reject( e );
            };
            return deferred.promise();
        };

        this.clear = function ( table ) {
            if ( closed ) {
                throw 'Database has been closed';
            }
            var transaction = db.transaction( table , transactionModes.readwrite ),
                store = transaction.objectStore( table ),
                deferred = Deferred();

            var req = store.clear();
            transaction.oncomplete = function ( ) {
                deferred.resolve( );
            };
            transaction.onerror = function (e) {
                console.error(e);
                deferred.reject( e );
            };
            return deferred.promise();
        };
        
        this.close = function ( ) {
            if ( closed ) {
                throw 'Database has been closed';
            }
            db.close();
            closed = true;
            delete dbCache[ name ];
        };

        this.get = function (table, id) {
            if (closed) {
                throw 'Database has been closed';
            }

            var transaction = db.transaction(table),
                store = transaction.objectStore(table),
                deferred = Deferred();

            var req = store.get( id );
            req.onsuccess = function ( e ) {
                deferred.resolve( e.target.result );
            };
            transaction.onerror = function (e) {
                console.error(e);
                deferred.reject( e );
            };

            return deferred.promise();
        };
        this.getExistKey = function (table) {
            if (closed) {
                throw 'Database has been closed';
            }
            //Tam: allow get multiple keys 
            var keys = [];
            for (var i = 0 ; i < arguments.length - 1 ; i++) {
                keys[i] = arguments[i + 1];
            }

            var transaction = db.transaction(table),
                store = transaction.objectStore(table),
                deferred = Deferred();
            var results = [];
            function getNext(dropbox, df) {
                if (!keys.length) {
                    //return result
                    df.resolve(dropbox);
                    return;
                }
                var k = keys.shift();
                var req = store.get(k);
                req.onsuccess = function (e) {
                    var val = e.target.result;
                    if (val !== undefined) {
                        dropbox.push(val);
                    }
                    getNext(dropbox, df);
                };
            }

            transaction.onerror = function (e) {
                console.error(e);
                deferred.reject(e);
            };
            getNext(results, deferred);
            return deferred.promise();
        };        

        //get all existing entity / keys 
        //by default, sort incremently if keyOnly is passed in
        this.getAll = function (table, keys, keyOnly) {
            if ( closed ) {
                throw 'Database has been closed';
            }
            /*
            var keys = [];
            for (var i = 0 ; i < arguments.length - 1 ; i++) {
                keys[i] = arguments[i + 1];
            }
            */
            //keys = keys.getUnique();

            var transaction = db.transaction( table ),
                store = transaction.objectStore( table ),
                deferred = Deferred();
            var results = [];
            function getNext(dropbox,df) {
                if (!keys.length) {
                    //return result
                    df.resolve(dropbox);
                    return;
                }
                var k = keys.shift();
                var req = store.get(k);
                req.onsuccess = function (e) {
                    var val = e.target.result;
                    var keyPath = e.target.source.keyPath;
                    if ( keyPath === null ) {
                        keyPath = '__id__';
                    }

                    if (val!== undefined) {
                        if (keyOnly) {
                            var id = val[keyPath];
                            dropbox.insertIncrement(id);
                        }
                        else {
                            dropbox.push(val);
                        }
                    }
                    getNext(dropbox,df);
                };                
            }

            transaction.onerror = function (e) {
                console.error(e);
                deferred.reject(e);
            };
            getNext(results, deferred);            
            return deferred.promise();
        };

        this.query = function ( table , index ) {
            if ( closed ) {
                throw 'Database has been closed';
            }
            return new IndexQuery( table , db , index );
        };

        for ( var i = 0 , il = db.objectStoreNames.length ; i < il ; i++ ) {
            (function ( storeName ) {
                that[ storeName ] = { };
                for ( var i in that ) {
                    if ( !hasOwn.call( that , i ) || i === 'close' ) {
                        continue;
                    }
                    that[ storeName ][ i ] = (function ( i ) {
                        return function () {
                            var args = [ storeName ].concat( [].slice.call( arguments , 0 ) );
                            return that[ i ].apply( that , args );
                        };
                    })( i );
                }
            })( db.objectStoreNames[ i ] );
        }
    };

    var IndexQuery = function ( table , db , indexName ) {
        var that = this;
        var modifyObj = false;

        var runQuery = function (type, args, cursorType, direction, limitRange, filters, mapper) {
            //console.log('run query with type = ' + type + ' cursor type = ' + cursorType + ' dir = ' + direction + ' indexName = ' + indexName);
            //console.log(args);
            var transaction = db.transaction( table, modifyObj ? transactionModes.readwrite : transactionModes.readonly ),
                store = transaction.objectStore( table ),
                index = indexName ? store.index( indexName ) : store,
                keyRange = type ? IDBKeyRange[ type ].apply( null, args ) : null,
                results = [],
                deferred = Deferred(),
                indexArgs = [ keyRange ],
                limitRange = limitRange ? limitRange : null,
                filters = filters ? filters : [],
                counter = 0;

            if ( cursorType !== 'count' ) {
                indexArgs.push( direction || 'next' );
            };

            // create a function that will set in the modifyObj properties into
            // the passed record.
            var modifyKeys = modifyObj ? Object.keys(modifyObj) : false;
            var modifyRecord = function(record) {
                for(var i = 0; i < modifyKeys.length; i++) {
                    var key = modifyKeys[i];
                    var val = modifyObj[key];
                    if(val instanceof Function) val = val(record);
                    record[key] = val;
                }
                return record;
            };

            index[cursorType].apply( index , indexArgs ).onsuccess = function ( e ) {
                var cursor = e.target.result;
                if ( typeof cursor === typeof 0 ) {
                    results = cursor;
                } else if ( cursor ) {
                	if ( limitRange !== null && limitRange[0] > counter) {
                    	counter = limitRange[0];
                    	cursor.advance(limitRange[0]);
                    } else if ( limitRange !== null && counter >= (limitRange[0] + limitRange[1]) ) {
                        //out of limit range... skip
                    } else {
                        var matchFilter = true;
                        var result = 'value' in cursor ? cursor.value : cursor.key;

                        filters.forEach( function ( filter ) {
                            if ( !filter || !filter.length ) {
                                //Invalid filter do nothing
                            } else if ( filter.length === 2 ) {
                                matchFilter = matchFilter && (result[filter[0]] === filter[1])
                            } else {
                                matchFilter = matchFilter && filter[0].apply(undefined,[result]);
                            }
                        });

                        if (matchFilter) {
                            counter++;
                            results.push( mapper(result) );
                            // if we're doing a modify, run it now
                            if(modifyObj) {
                                result = modifyRecord(result);
                                cursor.update(result);
                            }
                        }
                        cursor.continue();
                    }
                }
            };

            transaction.oncomplete = function () {
                deferred.resolve( results );
            };
            transaction.onerror = function ( e ) {
                deferred.reject( e );
            };
            transaction.onabort = function ( e ) {
                deferred.reject( e );
            };
            return deferred.promise();
        };

        var Query = function ( type , args ) {
            var direction = 'next',
                cursorType = 'openCursor',
                filters = [],
                limitRange = null,
                mapper = defaultMapper,
                unique = false;

            var execute = function () {
                return runQuery( type , args , cursorType , unique ? direction + 'unique' : direction, limitRange, filters , mapper );
            };

            var limit = function () {
                limitRange = Array.prototype.slice.call( arguments , 0 , 2 )
                if (limitRange.length == 1) {
                    limitRange.unshift(0)
                }

                return {
                    execute: execute
                };
            };
            var count = function () {
                direction = null;
                cursorType = 'count';

                return {
                    execute: execute
                };
            };
            var keys = function () {
                cursorType = 'openKeyCursor';

                return {
                    desc: desc,
                    execute: execute,
                    filter: filter,
                    distinct: distinct,
                    map: map
                };
            };
            var filter = function ( ) {
                filters.push( Array.prototype.slice.call( arguments , 0 , 2 ) );

                return {
                    keys: keys,
                    execute: execute,
                    filter: filter,
                    desc: desc,
                    distinct: distinct,
                    modify: modify,
                    limit: limit,
                    map: map
                };
            };
            var desc = function () {
                direction = 'prev';

                return {
                    keys: keys,
                    execute: execute,
                    filter: filter,
                    distinct: distinct,
                    modify: modify,
                    map: map,
                    limit: limit 
                };
            };
            var distinct = function () {
                unique = true;
                return {
                    keys: keys,
                    count: count,
                    execute: execute,
                    filter: filter,
                    desc: desc,
                    modify: modify,
                    map: map
                };
            };
            var modify = function(update) {
                modifyObj = update;
                return {
                    execute: execute
                };
            };
            var map = function (fn) {
                mapper = fn;

                return {
                    execute: execute,
                    count: count,
                    keys: keys,
                    filter: filter,
                    desc: desc,
                    distinct: distinct,
                    modify: modify,
                    limit: limit,
                    map: map
                };
            };

            return {
                execute: execute,
                count: count,
                keys: keys,
                filter: filter,
                desc: desc,
                distinct: distinct,
                modify: modify,
                limit: limit,
                map: map
            };
        };
        
        'only bound upperBound lowerBound'.split(' ').forEach(function (name) {
            that[name] = function () {
                return new Query( name , arguments );
            };
        });

        this.filter = function () {
            var query = new Query( null , null );
            return query.filter.apply( query , arguments );
        };

        this.all = function () {
            return this.filter();
        };
    };
    
    var createSchema = function ( e , schema , db ) {
        if ( typeof schema === 'function' ) {
            schema = schema();
        }
        
        for ( var tableName in schema ) {
            var table = schema[ tableName ];
            var store;
            if (!hasOwn.call(schema, tableName) || db.objectStoreNames.contains(tableName)) {
                store = e.currentTarget.transaction.objectStore(tableName);
            } else {
                store = db.createObjectStore(tableName, table.key);
            }

            for ( var indexKey in table.indexes ) {
				if (store.indexNames.contains(indexKey)) {
                    continue;
                }
                var index = table.indexes[ indexKey ];
                store.createIndex( indexKey , index.key || indexKey , Object.keys(index).length ? index : { unique: false } );
            }
        }
    };
    
    var open = function ( e , server , version , schema ) {
        var db = e.target.result;
        var s = new Server( db , server );
        var upgrade;

        var deferred = Deferred();
        deferred.resolve( s );
        dbCache[ server ] = db;

        return deferred.promise();
    };

    var dbCache = {};

    var db = {
        version: '0.9.0',
        open: function ( options ) {
            var request;

            var deferred = Deferred();

            if ( dbCache[ options.server ] ) {
                open( {
                    target: {
                        result: dbCache[ options.server ]
                    }
                } , options.server , options.version , options.schema )
                .done(deferred.resolve)
                .fail(deferred.reject)
                .progress(deferred.notify);
            } else {
                request = indexedDB.open( options.server , options.version );
                            
                request.onsuccess = function ( e ) {
                    open( e , options.server , options.version , options.schema )
                        .done(deferred.resolve)
                        .fail(deferred.reject)
                        .progress(deferred.notify);
                };
            
                request.onupgradeneeded = function ( e ) {
                    createSchema( e , options.schema , e.target.result );
                };
                request.onerror = function ( e ) {
                    deferred.reject( e );
                };

                request.onblocked = function (e) {
                    alert("Please close all other tabs with this site open!");
                };
            }

            return deferred.promise();
        }, 

        clear: function(dbName){
            var request; 
            if ( dbCache[dbName] ) {
                try{
                    dbCache[dbName].close(); 
                    delete dbCache[dbName];        
                }
                catch(e){
                    console.error(e);
                }
            }
            var deferred = Deferred();
            request = window.indexedDB.clearDatabase(dbName)
            request.onsuccess = function (e){
                deferred.resolve(e);                 
            }

            request.onerror = function (e){
                deferred.reject(e);
            }
            return deferred.promise(); 
        }
    };

    if (typeof module !== 'undefined' && typeof module.exports !== 'undefined') {
        module.exports = db;
    } else if ( typeof define === 'function' && define.amd ) {
        define( function() { return db; } );
    } else {
        window.db = db;
    }
})( window );
