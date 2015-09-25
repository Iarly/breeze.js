(function (factory) {
  if (typeof breeze === "object") {
        factory(breeze);
    } else if (typeof require === "function" && typeof exports === "object" && typeof module === "object") {
        // CommonJS or Node: hard-coded dependency on "breeze"
        factory(require("breeze"));
  } else if (typeof define === "function" && define["amd"]) {
        // AMD anonymous module with hard-coded dependency on "breeze"
        define(["breeze"], factory);
    }
}(function (breeze) {
    "use strict";    
    var core = breeze.core;
 
    var MetadataStore = breeze.MetadataStore;
    var JsonResultsAdapter = breeze.JsonResultsAdapter;
    var DataProperty = breeze.DataProperty;
    
    var OData;
    
  var ctor = function DataServiceODataAdapter() {
        this.name = "OData";
    };

  var proto = ctor.prototype; // minifies better (as seen in jQuery)

  proto.initialize = function () {
        OData = core.requireLib("OData", "Needed to support remote OData services");
        OData.jsonHandler.recognizeDates = true;
    };
  // borrow from AbstractDataServiceAdapter
  var abstractDsaProto = breeze.AbstractDataServiceAdapter.prototype;
  proto._catchNoConnectionError = abstractDsaProto._catchNoConnectionError;
  proto.changeRequestInterceptor = abstractDsaProto.changeRequestInterceptor;
  proto._createChangeRequestInterceptor = abstractDsaProto._createChangeRequestInterceptor;
  proto.headers = { "DataServiceVersion": "2.0" };
    
  proto.getAbsoluteUrl = function (dataService, url){
      var serviceName = dataService.qualifyUrl( '' );
    // only prefix with serviceName if not already on the url
    var base = (core.stringStartsWith(url, serviceName)) ? '' : serviceName;
    // If no protocol, turn base into an absolute URI
    if (window && serviceName.indexOf('//') < 0) { 
      // no protocol; make it absolute
      base = window.location.protocol + '//' + window.location.host + 
            (core.stringStartsWith(serviceName, '/') ? '' : '/') +
            base;
    }
    return base + url;
  };
    
  // getRoutePrefix deprecated in favor of getAbsoluteUrl which seems to work for all OData providers; doubt anyone ever changed it; we'll see
  // TODO: Remove from code base soon (15 June 2015)
  // proto.getRoutePrefix = function (dataService) { return '';}   
    
  proto.executeQuery = function (mappingContext) {

        var deferred = Q.defer();
    var url = this.getAbsoluteUrl(mappingContext.dataService, mappingContext.getUrl());
        
        var paramSeparation = '?';
        if (url.indexOf('?') > -1)
            paramSeparation = '&';
        if (mappingContext.query && mappingContext.query.parameters) {
            var queryOptions = mappingContext.query.parameters;
            for (var qoName in queryOptions) {
                var qoValue = queryOptions[qoName];
                if (qoValue !== undefined) {
                    if (qoValue instanceof Array) {
                        qoValue.forEach(function (qov) {
                            url += paramSeparation + (qoName + "=" + encodeURIComponent(qov));
                        });
                    } else {
                        url += paramSeparation + (qoName + "=" + encodeURIComponent(qoValue));
                    }
                }
                paramSeparation = '&';
            }
        }

        var serviceUrl = this.getAbsoluteUrl(mappingContext.dataService, '');
        var methodUrl = url.replace(serviceUrl, '');

        OData.request({
            requestUri: serviceUrl + "/$batch",
            method: "POST",
            data: {
                __batchRequests: [
                   { requestUri: methodUrl, method: "GET" }
                ]
            }
        }, function (batchData, response) {
            if (batchData) {
                var response = batchData.__batchResponses[0];
                var data = response.data;
                if (response.statusCode == 200) {
                    var inlineCount;
                    if (data.__count) {
                        // OData can return data.__count as a string
                        inlineCount = parseInt(data.__count, 10);
                    }
                    return deferred.resolve({ results: data.results, inlineCount: inlineCount, httpResponse: response });
              }
          }
          return deferred.reject(createError({ response: response }, url));
        }, function (error) {
            return deferred.reject(createError(error, url));
        }, OData.batchHandler);
        return deferred.promise;
    };
    

  proto.fetchMetadata = function (metadataStore, dataService) {

        var deferred = Q.defer();

        var serviceName = dataService.serviceName;
    //var url = dataService.qualifyUrl('$metadata');
    var url = this.getAbsoluteUrl(dataService, '$metadata');

        if (dataService.customMetadataUrl) {
            url = dataService.customMetadataUrl;
        }

        OData.read({
          requestUri: url,
          // headers: { "Accept": "application/json"}
          headers: { Accept: 'application/json;odata.metadata=full' }
        },
        function (data, response) {
                // data.dataServices.schema is an array of schemas. with properties of 
                // entityContainer[], association[], entityType[], and namespace.
                if (!data || !data.dataServices) {
                    try {
                        data = JSON.parse(response.body);
                        if (data.schema) {
                            data = {
                                "version": "1.0",
                                "dataServices":
                                    {
                                        "dataServiceVersion": "1.0",
                                        "maxDataServiceVersion": "3.0",
                                        "schema": [data.schema]
                                    }
                            };
                        }
                    }
                    finally {
                        if (!data || !data.dataServices) {
                    var error = new Error("Metadata query failed for: " + url);
                    return deferred.reject(error);
                }
                    }
                }
                var csdlMetadata = data.dataServices;

                // might have been fetched by another query
                if (!metadataStore.hasMetadataFor(serviceName)) {
                    try {
                        metadataStore.importMetadata(csdlMetadata);
            } catch (e) {
                        return deferred.reject(new Error("Metadata query failed for " + url + "; Unable to process returned metadata: " + e.message));
                    }

                    metadataStore.addDataService(dataService);
                }

                return deferred.resolve(csdlMetadata);

            }, function (error) {
                var err = createError(error, url);
                err.message = "Metadata query failed for: " + url + "; " + (err.message || "");
                return deferred.reject(err);
            },
            OData.metadataHandler
        );

        return deferred.promise;

    };



  proto.saveChanges = function (saveContext, saveBundle) {
    var adapter = saveContext.adapter = this;
        var deferred = Q.defer();
        
        var helper = saveContext.entityManager.helper;
        var url = this.getAbsoluteUrl(saveContext.dataService, "$batch");

        var requestData = createChangeRequests(saveContext, saveBundle);
        var innerEntities = requestData.__innerEntities || [];
        delete requestData.__innerEntities;

        if (requestData.__batchRequests[0].__changeRequests.length == 0) {
            deferred.resolve({ entities: innerEntities, keyMappings: [] });
            return deferred.promise;
        }

        var requestData = createChangeRequests(saveContext, saveBundle);
        var tempKeys = saveContext.tempKeys;
        var contentKeys = saveContext.contentKeys;

        OData.request({
            headers: { "DataServiceVersion": "2.0" },
            requestUri: url,
            method: "POST",
            data: requestData
        }, function (data, response) {
            var statusCode = response.statusCode;
            if ((!statusCode) || statusCode == 203 || statusCode >= 400) {
                return deferred.reject(createError({ response: response }, url));
            }
            var entities = innerEntities;
            var keyMappings = [];
            var saveResult = { entities: entities, keyMappings: keyMappings };
      data.__batchResponses.forEach(function (br) {
                br.__changeResponses.forEach(function (cr) {
                    var response = cr.response || cr;
                    var statusCode = response.statusCode;
                    if ((!statusCode) || statusCode >= 400) {
                        deferred.reject(createError(cr, url));
                        return;
                    }
                    
                    var contentId = cr.headers["Content-ID"];
                    
                    var rawEntity = cr.data;
                    if (rawEntity) {
                        var tempKey = tempKeys[contentId];
                        if (tempKey) {
                            var entityType = tempKey.entityType;
                            if (entityType.autoGeneratedKeyType !== breeze.AutoGeneratedKeyType.None) {
                                var tempValue = tempKey.values[0];
                                var realKey = entityType.getEntityKeyFromRawEntity(rawEntity, DataProperty.getRawValueFromServer);
                                var keyMapping = { entityTypeName: entityType.name, tempValue: tempValue, realValue: realKey.values[0] };
                                keyMappings.push(keyMapping);
                            }
                        }
                        entities.push(rawEntity);
                    } else {
                        var origEntity = contentKeys[contentId];
                        if (origEntity) //
                            entities.push(origEntity);
                    }
                });
            });
            return deferred.resolve(saveResult);
        }, function (err) {
            return deferred.reject(createError(err, url));
        }, OData.batchHandler);

        return deferred.promise;

    };
 
  proto.jsonResultsAdapter = new JsonResultsAdapter({
        name: "OData_default",

        visitNode: function (node, mappingContext, nodeContext) {
            var result = {};
            if (node == null) return result;
            var metadata = node.__metadata;
            if (metadata != null) {
                // TODO: may be able to make this more efficient by caching of the previous value.
                var entityTypeName = MetadataStore.normalizeTypeName(metadata.type);
                var et = entityTypeName && mappingContext.entityManager.metadataStore.getEntityType(entityTypeName, true);

                    result.entityType = et;
                    var uriKey = metadata.uri || metadata.id;
          if (uriKey) {
            // Strip baseUri to make uriKey a relative uri
            // Todo: why is this necessary when absolute works for every OData source tested?
            var re = new RegExp('^' + mappingContext.dataService.serviceName, 'i');
            uriKey = uriKey.replace(re, '');
                    }
                    result.extraMetadata = {
                        uriKey: uriKey,
                        etag: metadata.etag
                    }
                }
            // OData v3 - projection arrays will be enclosed in a results array
            if (node.results) {
                result.node = node.results;
            }

            var propertyName = nodeContext.propertyName;
            result.ignore = node.__deferred != null || propertyName === "__metadata" ||
                // EntityKey properties can be produced by EDMX models
                (propertyName === "EntityKey" && node.$type && core.stringStartsWith(node.$type, "System.Data"));
            return result;
        }
        
    });

  function transformValue(prop, val) {
        if (prop.isUnmapped) return undefined;
        if (prop.dataType === breeze.DataType.DateTimeOffset) {
            // The datajs lib tries to treat client dateTimes that are defined as DateTimeOffset on the server differently
            // from other dateTimes. This fix compensates before the save.
            val = val && new Date(val.getTime() - (val.getTimezoneOffset() * 60000));
        } else if (prop.dataType.quoteJsonOData) {
            val = val != null ? val.toString() : val;
        }
        return val;
    }

    function _getEntityId(entity) {
        var prefix = "", sufix = "";
        if (entity.entityType.keyProperties[0].dataType.name == "Guid") {
            prefix = "guid'";
            sufix = "'";
        }
        return prefix + entity.entityAspect.getKey().values[0] + sufix;
    }

    function _getEntityUri(prefix, entity) {
        var extraMetadata = entity.entityAspect.extraMetadata;
        return extraMetadata != null ? (extraMetadata.uri || extraMetadata.id || extraMetadata.uriKey)
            : (location.origin + prefix + entity.entityType.defaultResourceName + "(" + _getEntityId(entity) + ")");
    }

    function createChangeRequests(saveContext, saveBundle) {
        var changeRequestInterceptor = saveContext.adapter._createChangeRequestInterceptor(saveContext, saveBundle);
        var innerEntities = [];
        var readedAssociations = [];
        var createdCREntities = [];
        var linksRequest = [];
        var changeRequests = [];
        var tempKeys = [];
        var contentKeys = [];
        var baseUri = saveContext.dataService.serviceName;
        var entityManager = saveContext.entityManager;
        var helper = entityManager.helper;
        var id = 0;
        var routePrefix = saveContext.routePrefix;

        saveBundle.entities.forEach(function (entity, index) {
            createdCREntities.push(entity);
            var aspect = entity.entityAspect;
            id = id + 1; // we are deliberately skipping id=0 because Content-ID = 0 seems to be ignored.
            var request = { headers: { "Content-ID": id, "DataServiceVersion": "2.0" } };
            contentKeys[id] = entity;

            if (entity.entityAspect.entityState.isModified()) {
                aspect.inseredLinks.forEach(function (inseredLink) {
                    if (!inseredLink.entity.entityAspect.entityState.isAdded()
                        && !inseredLink.entity.entityAspect.entityState.isDeleted()) {
                        var linkRequest = { headers: { "Content-ID": id, "DataServiceVersion": "3.0" } };
                        linkRequest.requestUri = _getEntityUri(baseUri, entity) + "/$links/" + inseredLink.np.name;
                        linkRequest.method = "POST";

                        var baseType = inseredLink.entity.entityType;
                        while (baseType.baseEntityType)
                            baseType = baseType.baseEntityType;

                        linkRequest.data = {
                            uri: (inseredLink.entity.entityAspect.extraMetadata ? inseredLink.entity.entityAspect.extraMetadata.uriKey : null) ||
                                baseUri + baseType.defaultResourceName + "(" + _getEntityId(inseredLink.entity) + ")"
                        };

                        linksRequest.push(linkRequest);
                    }
                });

                aspect.removedLinks.forEach(function (removedLink) {
                    if (createdCREntities.indexOf(removedLink.entity) == -1) {
                        if (!removedLink.entity.entityAspect.entityState.isAdded()
                            && !removedLink.entity.entityAspect.entityState.isDeleted()) {
                            var linkRequest = { headers: { "Content-ID": id, "DataServiceVersion": "3.0" } };
                            // DELETE /OData/OData.svc/Categories(1)/$links/Products(10)
                            linkRequest.requestUri = _getEntityUri(baseUri, aspect.entity)
                                + "/$links/" + removedLink.np.name + "(" + _getEntityId(removedLink.entity) + ")";
                            linkRequest.method = "DELETE";
                            linksRequest.push(linkRequest);
                        }
                    }
                });
            }

            if (aspect.entityState.isAdded()) {
                var options = { readedAssociations: readedAssociations };
                insertRequest(request, entity.entityType, routePrefix);
                request.method = "POST";
                request.data = helper.unwrapInstance(entity, transformValue, options);
                tempKeys[id] = aspect.getKey();
                // should be a PATCH/MERGE
                if (options.isIgnored || Object.keys(request.data).length == 0) {
                    innerEntities.push(entity);
                    id--;
                    return;
                }
            } else if (aspect.entityState.isModified()) {
                updateDeleteMergeRequest(request, aspect, baseUri, routePrefix);
                request.method = "MERGE";
                request.data = helper.unwrapChangedValues(entity, entityManager.metadataStore, transformValue);
                // should be a PATCH/MERGE
                if (Object.keys(request.data).length == 0) {
                    innerEntities.push(entity);
                    id--;
                    return;
                }
            } else if (aspect.entityState.isDeleted()) {
                updateDeleteMergeRequest(request, aspect, baseUri, routePrefix);
                request.method = "DELETE";
            } else {
                return;
            }
            request = changeRequestInterceptor.getRequest(request, entity, index);
            changeRequests.push(request);
        });

        linksRequest.forEach(function (link) {
            link.headers["Content-ID"] = ++id;
            changeRequests.push(link);
        });

        saveContext.contentKeys = contentKeys;
        saveContext.tempKeys = tempKeys;
        changeRequestInterceptor.done(changeRequests);
        
        return {
            __innerEntities: innerEntities,
            __batchRequests: [{
                __changeRequests: changeRequests
            }]
        };

    }

    function insertRequest(request, entityType, routePrefix) {
        var lastBaseType = null;
        var sbaseType = entityType;
        var defaultResourceName = entityType.defaultResourceName;
        do {
            lastBaseType = sbaseType;
            sbaseType = sbaseType.baseEntityType;
        } while (sbaseType);

        if (lastBaseType !== entityType) {
            var namespace = entityType.namespace;
            var className = entityType.name.replace(":#" + namespace, "");
            defaultResourceName = lastBaseType.defaultResourceName + "/" + namespace + "." + className;
        }

        routePrefix = routePrefix || '';
        request.requestUri = routePrefix + defaultResourceName;
    }

    function updateDeleteMergeRequest(request, aspect, routePrefix) {
        var uriKey;
        var extraMetadata = aspect.extraMetadata;
        if (extraMetadata == null) {
          uriKey = getUriKey(aspect);
          aspect.extraMetadata = {
            uriKey: uriKey
          }
        } else {
          uriKey = extraMetadata.uriKey;
          if (extraMetadata.etag) {
            request.headers["If-Match"] = extraMetadata.etag;
          }
        }
        request.requestUri =
          // use routePrefix if uriKey lacks protocol (i.e., relative uri)
          uriKey.indexOf('//') > 0 ? uriKey : routePrefix + uriKey;
    }

  function getUriKey(aspect) {
    var entityType = aspect.entity.entityType;
    var resourceName = entityType.defaultResourceName;
    var kps = entityType.keyProperties;
    var uriKey = resourceName + "(";
    if (kps.length === 1) {
      uriKey = uriKey + fmtProperty(kps[0], aspect) + ")";
    } else {
      var delim = "";
      kps.forEach(function (kp) {
        uriKey = uriKey + delim + kp.nameOnServer + "=" + fmtProperty(kp, aspect);
        delim = ",";
      });
      uriKey = uriKey + ")";
        }
        return uriKey;
    }

    function fmtProperty(prop, aspect) {
        return prop.dataType.fmtOData(aspect.getPropertyValue(prop.name));
    }
   
    function createError(error, url) {
        // OData errors can have the message buried very deeply - and nonobviously
        // this code is tricky so be careful changing the response.body parsing.
        var result = new Error();
        var response = error && error.response;
        if (!response) {
            // in case DataJS returns "No handler for this data"
            result.message = error;
            result.statusText = error;
            return result;
        }
        result.message = response.statusText;
        result.statusText = response.statusText;
        result.status = response.statusCode;
        // non std
        if (url) result.url = url;
        result.body = response.body;
        if (response.body) {
            var nextErr;
            try {
                var body = JSON.parse(response.body);
                result.body = body;
                // OData v3 logic
                if (body['odata.error']) {
                    body = body['odata.error'];
                }
                var msg = "";
                do {
                    nextErr = body.error || body.innererror;
                    if (!nextErr) msg = msg + getMessage(body);
                    nextErr = nextErr || body.internalexception;
                    body = nextErr || body;
                } while (nextErr);
                if (msg.length > 0) {
                    result.message = msg;
                }
            } catch (e) {

            }
        }
        proto._catchNoConnectionError(result);
        return result;
    }

    function getMessage(body) {
        var msg = body.message || "";
        return ((typeof (msg) === "string") ? msg : msg.value) + "; ";
    }

    breeze.config.registerAdapter("dataService", ctor);


    var webApiODataCtor = function () {
        this.name = "webApiOData";
    }

  breeze.core.extend(webApiODataCtor.prototype, proto);

/*
  // Deprecated in favor of getAbsoluteUrl
  // TODO: Remove from code base soon (15 June 2015)
  webApiODataCtor.prototype.getRoutePrefix = function (dataService) {
        // Get the routePrefix from a Web API OData service name.
    // The routePrefix is presumed to be the pathname within the dataService.serviceName
    // Examples of servicename -> routePrefix:
    //   'http://localhost:55802/odata/' -> 'odata/'
    //   'http://198.154.121.75/service/odata/' -> 'service/odata/'
    var parser;
    if (typeof document === 'object') { // browser
      parser = document.createElement('a');
      parser.href = dataService.serviceName;
    } else { // node
      parser = url.parse(dataService.serviceName);
    }
    var prefix = parser.pathname;
    if (prefix[0] === '/') {
      prefix = prefix.substr(1);
    } // drop leading '/'  (all but IE)
    if (prefix.substr(-1) !== '/') {
      prefix += '/';
    }      // ensure trailing '/'
    return prefix;
    };
  */

    breeze.config.registerAdapter("dataService", webApiODataCtor);
  // OData 4 adapter
  var webApiOData4Ctor = function () {
    this.name = "webApiOData4";
  }
  breeze.core.extend(webApiOData4Ctor.prototype, webApiODataCtor.prototype);
  webApiOData4Ctor.prototype.initialize = function () {
    // Aargh... they moved the cheese.
    var datajs = core.requireLib("datajs", "Needed to support remote OData v4 services");
    OData = datajs.V4.oData;
    OData.json.jsonHandler.recognizeDates = true;
  };
  webApiOData4Ctor.prototype.headers = { "OData-Version": "4.0" };
  breeze.config.registerAdapter("dataService", webApiOData4Ctor);


}));