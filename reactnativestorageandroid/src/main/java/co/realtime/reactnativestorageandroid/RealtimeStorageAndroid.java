package co.realtime.reactnativestorageandroid;

import android.support.annotation.Nullable;
import android.util.Log;

import com.facebook.react.bridge.Callback;
import com.facebook.react.bridge.ReactApplicationContext;
import com.facebook.react.bridge.ReactContext;
import com.facebook.react.bridge.ReactContextBaseJavaModule;
import com.facebook.react.bridge.ReactMethod;
import com.facebook.react.bridge.ReadableArray;
import com.facebook.react.bridge.ReadableMap;
import com.facebook.react.bridge.ReadableMapKeySeyIterator;
import com.facebook.react.bridge.WritableMap;
import com.facebook.react.bridge.WritableNativeMap;
import com.facebook.react.modules.core.DeviceEventManagerModule;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import co.realtime.storage.ItemAttribute;
import co.realtime.storage.ItemRef;
import co.realtime.storage.ItemSnapshot;
import co.realtime.storage.StorageRef;
import co.realtime.storage.TableRef;
import co.realtime.storage.TableSnapshot;
import co.realtime.storage.entities.TableMetadata;
import co.realtime.storage.ext.OnBooleanResponse;
import co.realtime.storage.ext.OnError;
import co.realtime.storage.ext.OnItemSnapshot;
import co.realtime.storage.ext.OnReconnected;
import co.realtime.storage.ext.OnReconnecting;
import co.realtime.storage.ext.OnTableCreation;
import co.realtime.storage.ext.OnTableMetadata;
import co.realtime.storage.ext.OnTableSnapshot;
import co.realtime.storage.ext.OnTableUpdate;
import co.realtime.storage.ext.StorageException;

@SuppressWarnings("all")

/**
 * Created by jcaixinha on 22/09/15.
 */
public class RealtimeStorageAndroid extends ReactContextBaseJavaModule {
    private HashMap<Integer, StorageRef> _storagerefs = new HashMap();
    private HashMap<Integer, HashMap<String, TableRef>> _tableRefs = new HashMap();
    private HashMap<Integer, ItemRef> _itemRefs = new HashMap();


    public RealtimeStorageAndroid(ReactApplicationContext reactContext) {
        super(reactContext);
    }

    @Override
    public String getName(){
        return "RealtimeCloudStorage";
    }


//===========================================================================================================



    private StorageRef.StorageProvisionLoad convertProvisionLoad(String ProvisionLoadString)
    {
        try {
            HashMap<String, StorageRef.StorageProvisionLoad> enu = new HashMap();
            enu.put("ProvisionLoad_READ", StorageRef.StorageProvisionLoad.READ);
            enu.put("ProvisionLoad_WRITE", StorageRef.StorageProvisionLoad.WRITE);
            enu.put("ProvisionLoad_BALANCED", StorageRef.StorageProvisionLoad.BALANCED);

            StorageRef.StorageProvisionLoad res = enu.get(ProvisionLoadString);
            return res;
        }catch (NullPointerException e){
            return null;
        }
    }


    private StorageRef.StorageProvisionType convertProvisionType(String ProvisionTypeString)
    {
        try {
            HashMap<String, StorageRef.StorageProvisionType> enu = new HashMap();
            enu.put("ProvisionType_LIGHT", StorageRef.StorageProvisionType.LIGHT);
            enu.put("ProvisionType_MEDIUM", StorageRef.StorageProvisionType.MEDIUM);
            enu.put("ProvisionType_INTERMEDIATE", StorageRef.StorageProvisionType.INTERMEDIATE);
            enu.put("ProvisionType_HEAVY", StorageRef.StorageProvisionType.HEAVY);
            enu.put("ProvisionType_CUSTOM", StorageRef.StorageProvisionType.CUSTOM);

            StorageRef.StorageProvisionType res = enu.get(ProvisionTypeString);
            return res;
        }catch (NullPointerException e){
            return null;
        }
    }

    private StorageRef.StorageDataType convertStorageDataType(String StorageDataTypeString)
    {
        try {
            HashMap<String, StorageRef.StorageDataType> enu = new HashMap();
            enu.put("StorageDataType_STRING", StorageRef.StorageDataType.STRING);
            enu.put("StorageDataType_NUMBER", StorageRef.StorageDataType.NUMBER);

            StorageRef.StorageDataType res = enu.get(StorageDataTypeString);
            return res;
        }catch (NullPointerException e){
            return null;
        }
    }


    private StorageRef.StorageEvent convertStorageEvent(String StorageEventString)
    {
        try {
            HashMap<String, StorageRef.StorageEvent> enu = new HashMap();
            enu.put("StorageEvent_PUT", StorageRef.StorageEvent.PUT);
            enu.put("StorageEvent_UPDATE", StorageRef.StorageEvent.UPDATE);
            enu.put("StorageEvent_DELETE", StorageRef.StorageEvent.DELETE);

            StorageRef.StorageEvent res = enu.get(StorageEventString);
            return res;
        }catch (NullPointerException e){
            return null;
        }
    }


//===========================================================================================================



    @ReactMethod
    public void storageRef(String aApplicationKey, String aAuthenticationToken, Integer sId){
        StorageRef storageRef = _storagerefs.get(sId);
        if (storageRef == null){
            try {
                storageRef = new StorageRef(aApplicationKey, aAuthenticationToken);
            } catch (StorageException e) {
                e.printStackTrace();
            }
        }
        _storagerefs.put(sId, storageRef);
    }

    @ReactMethod
    public void storageRefCustom(String applicationKey, String authenticationToken, boolean isCluster, boolean isSecure, String url, Integer sId){
        StorageRef storageRef = _storagerefs.get(sId);
        if (storageRef == null){
            try {
                storageRef = new StorageRef(applicationKey, authenticationToken, isCluster, isSecure, url);
            } catch (StorageException e) {
                e.printStackTrace();
            }
        }
        _storagerefs.put(sId, storageRef);
    }




/*    @ReactMethod
    public void storageRef(String applicationKey,
                           String authenticationToken,
                           boolean isCluster,
                           boolean isSecure,
                           String url,
                           Heartbeat heartbeat,
                           String googleProjectId,
                           Context androidApplicationContext,
                           String sId){
        StorageRef storageRef = _storagerefs.get(sId);
        if (storageRef == null){
            try {
                storageRef = new StorageRef(applicationKey,
                                                        authenticationToken,
                                                        isCluster,
                                                        isSecure,
                                                        url,
                                                        heartbeat,
                                                        googleProjectId,
                                                        androidApplicationContext);
            } catch (StorageException e) {
                e.printStackTrace();
            }
        }
        _storagerefs.put(sId, storageRef);
    }

    @ReactMethod
    public void storageRef(String applicationKey, String authenticationToken, String googleProjectId, Context androidApplicationContext, String sId){
        StorageRef storageRef = _storagerefs.get(sId);
        if (storageRef == null){
            try {
                storageRef = new StorageRef(applicationKey, authenticationToken, googleProjectId, androidApplicationContext);
            } catch (StorageException e) {
                e.printStackTrace();
            }
        }
        _storagerefs.put(sId, storageRef);
    }*/





    @ReactMethod
    public void getTables(final Integer sId){
        StorageRef storageRef = _storagerefs.get(sId);
        if (storageRef == null)
            return;

        storageRef.getTables(new OnTableSnapshot() {
            @Override
            public void run(TableSnapshot tableSnapshot) {
                WritableNativeMap map = new WritableNativeMap();
                map.putString("tableSnapshot", tableSnapshot.val());
                sendEvent(getReactApplicationContext(), sId + "-getTables", map);
            }
        }, new OnError() {
            @Override
            public void run(Integer integer, String s) {
                WritableNativeMap map = new WritableNativeMap();
                map.putString("error", s);
                sendEvent(getReactApplicationContext(), sId + "-getTables", map);
            }
        });
    }


    @ReactMethod
    public void isAuthenticated(String token, final Integer sId){
        StorageRef storageRef = _storagerefs.get(sId);
        if (storageRef == null)
            return;

        storageRef.isAuthenticated(token, new OnBooleanResponse() {
            @Override
            public void run(Boolean aBoolean) {
                WritableNativeMap map = new WritableNativeMap();
                map.putString("success", "" + aBoolean);
                sendEvent(getReactApplicationContext(), sId + "-isAuthenticated", map);
            }
        }, new OnError() {
            @Override
            public void run(Integer integer, String s) {
                WritableNativeMap map = new WritableNativeMap();
                map.putString("error", s);
                sendEvent(getReactApplicationContext(), sId + "-isAuthenticated", map);
            }
        });
    }

    @ReactMethod
    public void table(String tableName, Integer sId, String tId){
        StorageRef storageRef = _storagerefs.get(sId);
        if (storageRef == null)
            return;

        TableRef tableRef;
        try{
            tableRef = storageRef.table(tableName);
            appendTable(tableRef, sId, tId);
        }catch (NullPointerException e){

            return;
        }
    }


    private void appendTable(TableRef table, Integer sId, String tId){
        HashMap<String, TableRef> objs = _tableRefs.get(sId);
        if (objs == null)
            objs = new HashMap();

        objs.put(tId, table);
        _tableRefs.put(sId, objs);
    }


    @ReactMethod
    public void onReconnect(final Integer sId){
        StorageRef storageRef = _storagerefs.get(sId);
        if (storageRef == null)
            return;

        storageRef.onReconnected(new OnReconnected() {
            @Override
            public void run(StorageRef storageRef) {
                sendEvent(getReactApplicationContext(), sId + "-onReconnect", null);
            }
        });
    }

    @ReactMethod
    public void onReconnecting(final Integer sId){
        StorageRef storageRef = _storagerefs.get(sId);
        if (storageRef == null)
            return;

        storageRef.onReconnecting(new OnReconnecting() {
            @Override
            public void run(StorageRef storageRef) {
                sendEvent(getReactApplicationContext(), sId + "-onReconnecting", null);
            }
        });
    }

    @ReactMethod
    public void activateOfflineBuffering(final Integer sId){
        StorageRef storageRef = _storagerefs.get(sId);
        if (storageRef == null)
            return;
        storageRef.activateOfflineBuffering();
    }

    @ReactMethod
    public void deactivateOfflineBuffering(final Integer sId){
        StorageRef storageRef = _storagerefs.get(sId);
        if (storageRef == null)
            return;
        storageRef.deactivateOfflineBuffering();
    }

    //===========================================================================================================

    /**
     * TableRef interface
     */

    @ReactMethod
    public void asc(final Integer sId, String table){
        TableRef tableRef = _tableRefs.get(sId).get(table);
        if (tableRef == null)
            return;
        TableRef res = tableRef.asc();
    }

    @ReactMethod
    public void desc(final Integer sId, String table){
        TableRef tableRef = _tableRefs.get(sId).get(table);
        if (tableRef == null)
            return;
        TableRef res = tableRef.desc();
    }

    @ReactMethod
    public void beginsWithString(final String item, final String value, Integer sId, String table){
        TableRef tableRef = _tableRefs.get(sId).get(table);
        if (tableRef == null)
            return;
        TableRef res = tableRef.beginsWith(item, new ItemAttribute(value));
    }

    @ReactMethod
    public void beginsWithNumber(final String item, final String value, Integer sId, String table){
        TableRef tableRef = _tableRefs.get(sId).get(table);
        if (tableRef == null)
            return;
        if (value.contains(".")){
            tableRef.beginsWith(item, new ItemAttribute(Double.parseDouble(value)));
        }else{
            tableRef.beginsWith(item, new ItemAttribute(Integer.parseInt(value)));
        }

    }

    @ReactMethod
    public void betweenString(final String item, final String beginValue, final String endValue, Integer sId, String table){
        TableRef tableRef = _tableRefs.get(sId).get(table);
        if (tableRef == null)
            return;
        TableRef res = tableRef.between(item, new ItemAttribute(beginValue), new ItemAttribute(endValue));
    }

    @ReactMethod
    public void betweenNumber(final String item, final String beginValue, final String endValue, Integer sId, String table){
        TableRef tableRef = _tableRefs.get(sId).get(table);
        if (tableRef == null)
            return;
        TableRef res = tableRef.between(item, new ItemAttribute(Double.parseDouble(beginValue)), new ItemAttribute(Double.parseDouble(endValue)));
    }

    @ReactMethod
    public void containsString(final String item, final String value, Integer sId, String table){
        TableRef tableRef = _tableRefs.get(sId).get(table);
        if (tableRef == null)
            return;
        TableRef res = tableRef.contains(item, new ItemAttribute(value));
    }

    @ReactMethod
    public void containsNumber(final String item, final String value, Integer sId, String table){
        TableRef tableRef = _tableRefs.get(sId).get(table);
        if (tableRef == null)
            return;
        if (value.contains(".")){
            tableRef.contains(item, new ItemAttribute(Double.parseDouble(value)));
        }else{
            tableRef.contains(item, new ItemAttribute(Integer.parseInt(value)));
        }
        TableRef res = tableRef.contains(item, new ItemAttribute(value));
    }

    @ReactMethod
    public void equalsString(final String item, final String value, Integer sId, String table){
        TableRef tableRef = _tableRefs.get(sId).get(table);
        if (tableRef == null)
            return;

        TableRef res = tableRef.equals(item, new ItemAttribute(value));
    }

    @ReactMethod
    public void equalsNumber(final String item, final String value, Integer sId, String table){
        TableRef tableRef = _tableRefs.get(sId).get(table);
        if (tableRef == null)
            return;

        if (value.contains(".")) {
            TableRef res = tableRef.equals(item, new ItemAttribute(Double.parseDouble(value)));
        }else{
            TableRef res = tableRef.equals(item, new ItemAttribute(Integer.parseInt(value)));
        }
    }

    @ReactMethod
    public void greaterEqualString(final String item, final String value, Integer sId, String table){
        TableRef tableRef = _tableRefs.get(sId).get(table);
        if (tableRef == null)
            return;
        TableRef res = tableRef.greaterEqual(item, new ItemAttribute(value));
    }

    @ReactMethod
    public void greaterEqualNumber(final String item, final String value, Integer sId, String table){
        TableRef tableRef = _tableRefs.get(sId).get(table);
        if (tableRef == null)
            return;
        if (value.contains(".")) {
            TableRef res = tableRef.greaterEqual(item, new ItemAttribute(Double.parseDouble(value)));
        }else{
            TableRef res = tableRef.greaterEqual(item, new ItemAttribute(Integer.parseInt(value)));
        }
    }

    @ReactMethod
    public void greaterThanString(final String item, final String value, Integer sId, String table){
        TableRef tableRef = _tableRefs.get(sId).get(table);
        if (tableRef == null)
            return;
        TableRef res = tableRef.greaterThan(item, new ItemAttribute(value));
    }

    @ReactMethod
    public void greaterThanNumber(final String item, final String value, Integer sId, String table){
        TableRef tableRef = _tableRefs.get(sId).get(table);
        if (tableRef == null)
            return;
        if (value.contains(".")) {
            TableRef res = tableRef.greaterThan(item, new ItemAttribute(Double.parseDouble(value)));
        }else{
            TableRef res = tableRef.greaterThan(item, new ItemAttribute(Integer.parseInt(value)));
        }
    }

    @ReactMethod
    public void lesserEqualString(final String item, final String value, Integer sId, String table){
        TableRef tableRef = _tableRefs.get(sId).get(table);
        if (tableRef == null)
            return;
        TableRef res = tableRef.lessEqual(item, new ItemAttribute(value));
    }

    @ReactMethod
    public void lesserEqualNumber(final String item, final String value, Integer sId, String table){
        TableRef tableRef = _tableRefs.get(sId).get(table);
        if (tableRef == null)
            return;
        if (value.contains(".")) {
            TableRef res = tableRef.lessEqual(item, new ItemAttribute(Double.parseDouble(value)));
        }else{
            TableRef res = tableRef.lessEqual(item, new ItemAttribute(Integer.parseInt(value)));
        }
    }

    @ReactMethod
    public void lesserThanString(final String item, final String value, Integer sId, String table){
        TableRef tableRef = _tableRefs.get(sId).get(table);
        if (tableRef == null)
            return;
        TableRef res = tableRef.lessThan(item, new ItemAttribute(value));
    }

    @ReactMethod
    public void lesserThanNumber(final String item, final String value, Integer sId, String table){
        TableRef tableRef = _tableRefs.get(sId).get(table);
        if (tableRef == null)
            return;
        if (value.contains(".")) {
            TableRef res = tableRef.lessThan(item, new ItemAttribute(Double.parseDouble(value)));
        }else{
            TableRef res = tableRef.lessThan(item, new ItemAttribute(Integer.parseInt(value)));
        }
    }

    @ReactMethod
    public void notContainsString(final String item, final String value, Integer sId, String table){
        TableRef tableRef = _tableRefs.get(sId).get(table);
        if (tableRef == null)
            return;
        TableRef res = tableRef.notContains(item, new ItemAttribute(value));
    }

    @ReactMethod
    public void notContainsNumber(final String item, final String value, Integer sId, String table){
        TableRef tableRef = _tableRefs.get(sId).get(table);
        if (tableRef == null)
            return;
        if (value.contains(".")) {
            TableRef res = tableRef.notContains(item, new ItemAttribute(Double.parseDouble(value)));
        }else{
            TableRef res = tableRef.notContains(item, new ItemAttribute(Integer.parseInt(value)));
        }
    }

    @ReactMethod
    public void notEqualString(final String item, final String value, Integer sId, String table){
        TableRef tableRef = _tableRefs.get(sId).get(table);
        if (tableRef == null)
            return;
        TableRef res = tableRef.notContains(item, new ItemAttribute(value));
    }

    @ReactMethod
    public void notEqualNumber(final String item, final String value, Integer sId, String table){
        TableRef tableRef = _tableRefs.get(sId).get(table);
        if (tableRef == null)
            return;
        if (value.contains(".")) {
            TableRef res = tableRef.notContains(item, new ItemAttribute(Double.parseDouble(value)));
        }else{
            TableRef res = tableRef.notContains(item, new ItemAttribute(Integer.parseInt(value)));
        }
    }

    @ReactMethod
    public void notNull(final String item, Integer sId, String table){
        TableRef tableRef = _tableRefs.get(sId).get(table);
        if (tableRef == null)
            return;
        TableRef res = tableRef.notNull(item);
    }

    @ReactMethod
    public void Null(final String item, Integer sId, String table){
        TableRef tableRef = _tableRefs.get(sId).get(table);
        if (tableRef == null)
            return;
        TableRef res = tableRef.isNull(item);
    }

    @ReactMethod
    public void create(String aPrimaryKey, String aPrimaryKeyDataType, String aProvisionType, String aProvisionLoad, final Integer sId, final String table){
        StorageRef.StorageProvisionLoad pLoad = convertProvisionLoad(aProvisionLoad);
        StorageRef.StorageProvisionType pType = convertProvisionType(aProvisionType);
        StorageRef.StorageDataType sDType = convertStorageDataType(aPrimaryKeyDataType);

        TableRef tableRef = _tableRefs.get(sId).get(table);

        tableRef.create(aPrimaryKey, sDType, pType, pLoad, new OnTableCreation() {
            @Override
            public void run(String s, Double aDouble, String s1) {
                WritableNativeMap map = new WritableNativeMap();
                map.putString("table", "" + s);
                map.putString("date", "" + aDouble);
                map.putString("status", "" + s1);
                sendEvent(getReactApplicationContext(), table + "-create", map);
            }
        }, new OnError() {
            @Override
            public void run(Integer integer, String s) {
                WritableNativeMap map = new WritableNativeMap();
                map.putString("error", s);
                sendEvent(getReactApplicationContext(), table + "-create", map);
            }
        });
    }


    @ReactMethod
    public void createCustom(String aPrimaryKey, String aPrimaryKeyDataType, String aSecondaryKey, String aSecondaryKeyDataType, String aProvisionType, String aProvisionLoad, final Integer sId, final String table){
        try {
            StorageRef.StorageProvisionLoad pLoad = convertProvisionLoad(aProvisionLoad);
            StorageRef.StorageProvisionType pType = convertProvisionType(aProvisionType);

            StorageRef.StorageDataType sPType = convertStorageDataType(aPrimaryKeyDataType);
            StorageRef.StorageDataType sSType = convertStorageDataType(aPrimaryKeyDataType);

            TableRef tableRef = _tableRefs.get(sId).get(table);

            tableRef.create(aPrimaryKey, sPType, aSecondaryKey, sSType, pType, pLoad, new OnTableCreation() {
                @Override
                public void run(String s, Double aDouble, String s1) {
                    WritableNativeMap map = new WritableNativeMap();
                    map.putString("table", "" + s);
                    map.putString("date", "" + aDouble);
                    map.putString("status", "" + s1);
                    sendEvent(getReactApplicationContext(), table + "-create", map);
                }
            }, new OnError() {
                @Override
                public void run(Integer integer, String s) {
                    WritableNativeMap map = new WritableNativeMap();
                    map.putString("error", s);
                    sendEvent(getReactApplicationContext(), table + "-create", map);
                }
            });
        }catch (NullPointerException e){

        }
    }

    @ReactMethod
    public void del (final Integer sId, final String table){
        TableRef tableRef = _tableRefs.get(sId).get(table);

        tableRef.del(new OnBooleanResponse() {
            @Override
            public void run(Boolean aBoolean) {
                WritableNativeMap map = new WritableNativeMap();
                map.putString("result", "" + aBoolean);
                sendEvent(getReactApplicationContext(), table + "-del", map);
            }
        }, new OnError() {
            @Override
            public void run(Integer integer, String s) {
                WritableNativeMap map = new WritableNativeMap();
                map.putString("error", s);
                sendEvent(getReactApplicationContext(), table + "-del", map);
            }
        });
    }


    @ReactMethod
    public void update(String aProvisionType, String aProvisionLoad, final Integer sId, final String table){
        StorageRef.StorageProvisionLoad pLoad = convertProvisionLoad(aProvisionLoad);
        StorageRef.StorageProvisionType pType = convertProvisionType(aProvisionType);

        TableRef tableRef = _tableRefs.get(sId).get(table);
        tableRef.update(pLoad, pType, new OnTableUpdate() {
            @Override
            public void run(String s, String s1) {
                WritableNativeMap map = new WritableNativeMap();
                map.putString("table", "" + s);
                map.putString("status", "" + s1);
                sendEvent(getReactApplicationContext(), table + "-update", map);
            }
        }, new OnError() {
            @Override
            public void run(Integer integer, String s) {
                WritableNativeMap map = new WritableNativeMap();
                map.putString("error", s);
                sendEvent(getReactApplicationContext(), table + "-update", map);
            }
        });
    }


    @ReactMethod
    public void item(final String primaryKey, String primaryType, Integer sId, String table, final Integer iId){
        final TableRef tableRef = _tableRefs.get(sId).get(table);
        ItemAttribute primaryItem = null;

        if (primaryType.equals("string")) {
            primaryItem = new ItemAttribute(primaryKey);
        } else if (primaryType.equals("number")) {
            if (primaryKey.contains(".")){
                primaryItem = new ItemAttribute(Double.parseDouble(primaryKey));
            }else{
                primaryItem = new ItemAttribute(Integer.parseInt(primaryKey));
            }
        }

        ItemRef item = _itemRefs.get(iId);
        if (item == null)
            item = tableRef.item(primaryItem);

        _itemRefs.put(iId, item);
    }


    @ReactMethod
    public void itemCustom(final String primaryKey, String primaryType, final String secondaryKey, String secondaryType, Integer sId, String table, final Integer iId){
        final TableRef tableRef = _tableRefs.get(sId).get(table);
        ItemAttribute primaryItem = null;
        ItemAttribute secondaryItem = null;

        if (primaryType.equals("string")) {
            primaryItem = new ItemAttribute(primaryKey);
        } else if (primaryType.equals("number")) {
            if (primaryKey.contains(".")){
                primaryItem = new ItemAttribute(Double.parseDouble(primaryKey));
            }else{
                primaryItem = new ItemAttribute(Integer.parseInt(primaryKey));
            }
        }

        if (secondaryType.equals("string")) {
            secondaryItem = new ItemAttribute(secondaryKey);
        } else if (secondaryType.equals("number")){
            if (secondaryKey.contains(".")){
                secondaryItem = new ItemAttribute(Double.parseDouble(secondaryKey));
            }else{
                secondaryItem = new ItemAttribute(Integer.parseInt(secondaryKey));
            }
        }

        ItemRef item = _itemRefs.get(iId);
        if (item == null)
            item = tableRef.item(primaryItem, secondaryItem);

        _itemRefs.put(iId, item);
    }

    @ReactMethod
    public void push(ReadableMap aItem, Integer sId, String table){
        TableRef tableRef = _tableRefs.get(sId).get(table);
        aItem.keySetIterator();
        tableRef.push(convertReadableMap(aItem), new OnItemSnapshot() {
            @Override
            public void run(ItemSnapshot itemSnapshot) {

            }
        }, new OnError() {
            @Override
            public void run(Integer integer, String s) {

            }
        });
    }

    @ReactMethod
    public void getItems(final Integer sId, final String table){
        TableRef tableRef = _tableRefs.get(sId).get(table);
        tableRef.getItems(new OnItemSnapshot() {
            @Override
            public void run(ItemSnapshot itemSnapshot) {
                if (itemSnapshot != null) {
                    sendEvent(getReactApplicationContext(), table + "-getItems", convertlinkedMap(itemSnapshot.val()));
                }else
                    sendEvent(getReactApplicationContext(), table + "-getItems", null);
            }
        }, new OnError() {
            @Override
            public void run(Integer integer, String s) {
                WritableNativeMap map = new WritableNativeMap();
                map.putString("error", s);
                sendEvent(getReactApplicationContext(), table + "-getItems", map);
            }
        });
    }

    @ReactMethod
    public void limit(Integer value, final Integer sId, String table){
        TableRef tableRef = _tableRefs.get(sId).get(table);
        tableRef.limit((long)value);
    }

    @ReactMethod
    public void meta(final Integer sId, final String table){
        TableRef tableRef = _tableRefs.get(sId).get(table);
        tableRef.meta(new OnTableMetadata() {
            @Override
            public void run(TableMetadata tableMetadata) {
                sendEvent(getReactApplicationContext(), table + "-meta", tableMetadata.toString());
            }
        }, new OnError() {
            @Override
            public void run(Integer integer, String s) {
                WritableNativeMap map = new WritableNativeMap();
                map.putString("error", s);
                sendEvent(getReactApplicationContext(), table + "-meta", map);
            }
        });
    }

    @ReactMethod
    public void name(Callback callBack, final Integer sId, String table){
        TableRef tableRef = _tableRefs.get(sId).get(table);
        callBack.invoke(tableRef.name());
    }

    @ReactMethod
    public void on(final String eventType, final Integer sId, final String table){

        StorageRef.StorageEvent event = convertStorageEvent(eventType);
        TableRef tableRef = _tableRefs.get(sId).get(table);

        tableRef.on(event, new OnItemSnapshot() {
            @Override
            public void run(ItemSnapshot itemSnapshot) {
                sendEvent(getReactApplicationContext(), table + "-on-" + eventType, convertlinkedMap(itemSnapshot.val()));
            }
        });
    }


    @ReactMethod
    public void onCustom(final String eventType, String aPrimaryKeyValue, final Integer sId, final String table){

        StorageRef.StorageEvent event = convertStorageEvent(eventType);
        TableRef tableRef = _tableRefs.get(sId).get(table);

        tableRef.on(event, new ItemAttribute(aPrimaryKeyValue), new OnItemSnapshot() {
            @Override
            public void run(ItemSnapshot itemSnapshot) {
                sendEvent(getReactApplicationContext(), table + "-on-" + eventType, convertlinkedMap(itemSnapshot.val()));
            }
        });
    }

    @ReactMethod
    public void off(final String eventType, final Integer sId, String table){
        StorageRef.StorageEvent event = convertStorageEvent(eventType);
        TableRef tableRef = _tableRefs.get(sId).get(table);

        tableRef.off(event, new OnItemSnapshot() {
            @Override
            public void run(ItemSnapshot itemSnapshot) {

            }
        });
    }


    @ReactMethod
    public void offCustom(final String eventType, String aPrimaryKeyValue, final Integer sId, String table){
        StorageRef.StorageEvent event = convertStorageEvent(eventType);
        TableRef tableRef = _tableRefs.get(sId).get(table);

        tableRef.off(event, new ItemAttribute(aPrimaryKeyValue), new OnItemSnapshot() {
            @Override
            public void run(ItemSnapshot itemSnapshot) {

            }
        });
    }

    @ReactMethod
    public void once(final String eventType, final Integer sId, final String table){

        StorageRef.StorageEvent event = convertStorageEvent(eventType);
        TableRef tableRef = _tableRefs.get(sId).get(table);

        tableRef.once(event, new OnItemSnapshot() {
            @Override
            public void run(ItemSnapshot itemSnapshot) {
                sendEvent(getReactApplicationContext(), table + "-once-" + eventType, convertlinkedMap(itemSnapshot.val()));
            }
        }, new OnError() {
            @Override
            public void run(Integer integer, String s) {
                WritableNativeMap map = new WritableNativeMap();
                map.putString("error", s);
                sendEvent(getReactApplicationContext(), table + "-once-" + eventType, map);
            }
        });
    }


    @ReactMethod
    public void onceCustom(final String eventType, String aPrimaryKeyValue, final Integer sId, final String table){

        StorageRef.StorageEvent event = convertStorageEvent(eventType);
        TableRef tableRef = _tableRefs.get(sId).get(table);

        tableRef.once(event, new ItemAttribute(aPrimaryKeyValue), new OnItemSnapshot() {
            @Override
            public void run(ItemSnapshot itemSnapshot) {
                sendEvent(getReactApplicationContext(), table + "-once-" + eventType, convertlinkedMap(itemSnapshot.val()));
            }
        }, new OnError() {
            @Override
            public void run(Integer integer, String s) {
                WritableNativeMap map = new WritableNativeMap();
                map.putString("error", s);
                sendEvent(getReactApplicationContext(), table + "-once-" + eventType, map);
            }
        });
    }

    @ReactMethod
    public void enablePushNotifications(final Integer sId, String table){
        TableRef tableRef = _tableRefs.get(sId).get(table);
        tableRef.enablePushNotifications();
    }

    @ReactMethod
    public void disablePushNotifications(final Integer sId, String table){
        TableRef tableRef = _tableRefs.get(sId).get(table);
        tableRef.disablePushNotifications();
    }


//=========================================================

    @ReactMethod
    public void itemRefdel(Integer iId, final Callback success, final Callback error){
        ItemRef item = _itemRefs.get(iId);
        if (item == null)
            return;

        item.del(new OnItemSnapshot() {
            @Override
            public void run(ItemSnapshot itemSnapshot) {
                success.invoke(convertlinkedMap(itemSnapshot.val()));
            }
        }, new OnError() {
            @Override
            public void run(Integer integer, String s) {
                WritableNativeMap map = new WritableNativeMap();
                map.putString("error", s);
                error.invoke(map);
            }
        });
    }

    @ReactMethod
    public void itemRefget(Integer iId, final Callback success, final Callback error){
        ItemRef item = _itemRefs.get(iId);
        if (item == null)
            return;

        item.get(new OnItemSnapshot() {
            @Override
            public void run(ItemSnapshot itemSnapshot) {
                success.invoke(convertlinkedMap(itemSnapshot.val()));
            }
        }, new OnError() {
            @Override
            public void run(Integer integer, String s) {
                WritableNativeMap map = new WritableNativeMap();
                map.putString("error", s);
                error.invoke(map);
            }
        });
    }

    @ReactMethod
    public void itemRefset(ReadableMap attributes, Integer iId, final Callback success, final Callback error){
        ItemRef item = _itemRefs.get(iId);
        if (item == null)
            return;

        item.set(convertReadableMap(attributes), new OnItemSnapshot() {
            @Override
            public void run(ItemSnapshot itemSnapshot) {
                success.invoke(convertlinkedMap(itemSnapshot.val()));
            }
        }, new OnError() {
            @Override
            public void run(Integer integer, String s) {
                WritableNativeMap map = new WritableNativeMap();
                map.putString("error", s);
                error.invoke(map);
            }
        });
    }

    @ReactMethod
    public void itemRefincr(String property, String value, Integer iId, final Callback success, final Callback error){
        ItemRef item = _itemRefs.get(iId);
        if (item == null)
            return;
        Number number;
        if (value.contains(".")) {
            item.incr(property, Double.parseDouble(value), new OnItemSnapshot() {
                @Override
                public void run(ItemSnapshot itemSnapshot) {
                    success.invoke(convertlinkedMap(itemSnapshot.val()));
                }
            }, new OnError() {
                @Override
                public void run(Integer integer, String s) {
                    WritableNativeMap map = new WritableNativeMap();
                    map.putString("error", s);
                    error.invoke(map);
                }
            });
        }else{
            item.incr(property, Integer.parseInt(value), new OnItemSnapshot() {
                @Override
                public void run(ItemSnapshot itemSnapshot) {
                    success.invoke(convertlinkedMap(itemSnapshot.val()));
                }
            }, new OnError() {
                @Override
                public void run(Integer integer, String s) {
                    WritableNativeMap map = new WritableNativeMap();
                    map.putString("error", s);
                    error.invoke(map);
                }
            });
        }
    }

    @ReactMethod
    public void itemRefincrCustom(String property, Integer iId, final Callback success, final Callback error){
        ItemRef item = _itemRefs.get(iId);
        if (item == null)
            return;

        item.incr(property, new OnItemSnapshot() {
            @Override
            public void run(ItemSnapshot itemSnapshot) {
                success.invoke(convertlinkedMap(itemSnapshot.val()));
            }
        }, new OnError() {
            @Override
            public void run(Integer integer, String s) {
                WritableNativeMap map = new WritableNativeMap();
                map.putString("error", s);
                error.invoke(map);
            }
        });
    }


    @ReactMethod
    public void itemRefdecr(String property, String value, Integer iId, final Callback success, final Callback error){
        ItemRef item = _itemRefs.get(iId);
        if (item == null)
            return;

        if (value.contains(".")) {
            item.decr(property, Double.parseDouble(value), new OnItemSnapshot() {
                @Override
                public void run(ItemSnapshot itemSnapshot) {
                    success.invoke(convertlinkedMap(itemSnapshot.val()));
                }
            }, new OnError() {
                @Override
                public void run(Integer integer, String s) {
                    WritableNativeMap map = new WritableNativeMap();
                    map.putString("error", s);
                    error.invoke(map);
                }
            });
        }else{
            item.decr(property, Integer.parseInt(value), new OnItemSnapshot() {
                @Override
                public void run(ItemSnapshot itemSnapshot) {
                    success.invoke(convertlinkedMap(itemSnapshot.val()));
                }
            }, new OnError() {
                @Override
                public void run(Integer integer, String s) {
                    WritableNativeMap map = new WritableNativeMap();
                    map.putString("error", s);
                    error.invoke(map);
                }
            });
        }
    }


    @ReactMethod
    public void itemRefdecrCustom(String property, Integer iId, final Callback success, final Callback error){
        ItemRef item = _itemRefs.get(iId);
        if (item == null)
            return;

        item.decr(property, new OnItemSnapshot() {
            @Override
            public void run(ItemSnapshot itemSnapshot) {
                success.invoke(convertlinkedMap(itemSnapshot.val()));
            }
        }, new OnError() {
            @Override
            public void run(Integer integer, String s) {
                WritableNativeMap map = new WritableNativeMap();
                map.putString("error", s);
                error.invoke(map);
            }
        });
    }

    @ReactMethod
    public void itemRefon(String eventType, Integer iId, final Callback callBack){
        ItemRef item = _itemRefs.get(iId);
        if (item == null)
            return;

        item.on(convertStorageEvent(eventType), new OnItemSnapshot() {
            @Override
            public void run(ItemSnapshot itemSnapshot) {
                callBack.invoke(convertlinkedMap(itemSnapshot.val()));
            }
        });
    }

    @ReactMethod
    public void itemRefoff(String eventType, Integer iId){
        ItemRef item = _itemRefs.get(iId);
        if (item == null)
            return;

        item.off(convertStorageEvent(eventType), new OnItemSnapshot() {
            @Override
            public void run(ItemSnapshot itemSnapshot) {

            }
        });
    }


    @ReactMethod
    public void itemRefonce(String eventType, Integer iId, final Callback callBack){
        ItemRef item = _itemRefs.get(iId);
        if (item == null)
            return;

        item.once(convertStorageEvent(eventType), new OnItemSnapshot() {
            @Override
            public void run(ItemSnapshot itemSnapshot) {
                callBack.invoke(convertlinkedMap(itemSnapshot.val()));
            }
        });
    }

    @ReactMethod
    public void itemRefenablePushNotifications(Integer iId){
        ItemRef item = _itemRefs.get(iId);
        if (item == null)
            return;
        item.enablePushNotifications();
    }

    @ReactMethod
    public void itemRefdisablePushNotifications(Integer iId){
        ItemRef item = _itemRefs.get(iId);
        if (item == null)
            return;
        item.disablePushNotifications();
    }






    @ReactMethod
    public void log(String tag, String log){
        Log.i(tag, log);
    }

    private LinkedHashMap convertReadableMap(ReadableMap aItem){
        LinkedHashMap<String, ItemAttribute> map = new LinkedHashMap();
        ReadableMapKeySeyIterator ite = aItem.keySetIterator();

        while (ite.hasNextKey()){
            String key = ite.nextKey();
            try {
                map.put(key, new ItemAttribute(aItem.getString(key)));
            } catch (Exception e) {
                try {
                    map.put(key, new ItemAttribute(aItem.getInt(key)));
                } catch (Exception ee) {
                    map.put(key, new ItemAttribute(aItem.getDouble(key)));
                }
            }
        }
        return map;
    }




    private WritableMap convertlinkedMap(LinkedHashMap<String, ItemAttribute> map){
        WritableNativeMap wMap = new WritableNativeMap();
        Iterator<String> ite =  map.keySet().iterator();


        while (ite.hasNext()){
            String key = ite.next();
            ItemAttribute item =  map.get(key);
            try {
                if (item.isString())
                    wMap.putString(key, item.toString());
                else if (item.isNumber())
                    wMap.putDouble(key, Double.parseDouble(item.toString()));
            } catch (Exception e) {
                // Something went wrong!
            }
        }
        return wMap;
    }


    private void sendEvent(ReactContext reactContext,
                           String eventName,
                           @Nullable Object params) {
        reactContext
                .getJSModule(DeviceEventManagerModule.RCTDeviceEventEmitter.class)
                .emit(eventName, params);
    }
}
