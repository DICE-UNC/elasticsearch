package logindexer;

import java.beans.BeanInfo;
import java.beans.PropertyDescriptor;
import java.io.Reader;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.*;
import org.elasticsearch.common.transport.*;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHits;
import org.irods.jargon.core.pub.DataObjectAO;
import org.irods.jargon.core.pub.IRODSAccessObjectFactory;
import org.irods.jargon.core.pub.domain.AvuData;
import org.irods.jargon.core.pub.io.IRODSFile;
import org.irods.jargon.core.query.AVUQueryElement;
import org.irods.jargon.core.query.AVUQueryElement.AVUQueryPart;
import org.irods.jargon.core.query.AVUQueryOperatorEnum;
import org.irods.jargon.core.query.MetaDataAndDomainData;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.annotation.*;

import databook.edsl.map.Utils;
import databook.listener.*;
import databook.listener.Scheduler.Continuation;
import databook.listener.Scheduler.Job;
import databook.listener.service.IndexingService;
import databook.persistence.rule.EntityRule;
import databook.persistence.rule.RuleRegistry;
import databook.persistence.rule.rdf.ruleset.*;
import static org.elasticsearch.node.NodeBuilder.*;
import static org.elasticsearch.index.query.QueryBuilders.*;

public class ESIndexer extends AbstractIndexer implements Indexer {

	private static final boolean AS_CLIENT = true;
	Node node;
	Client client;
	public static Log log = LogFactory.getLog("index-indexer-elastic");

	private String getOSVersion() {
		String[] cmd = { "lsb_release", "-id" };

		String ret = "";
		try {
			Process p = Runtime.getRuntime().exec(cmd);
			BufferedReader bri = new BufferedReader(new InputStreamReader(
					p.getInputStream()));

			String line = "";
			while ((line = bri.readLine()) != null) {
				ret += line;
			}
		} catch (IOException e) {

			log.error("error", e);
		}

		return ret;

	}

	@Override
	public void startup() {
		super.startup();
		connect();

		Message m = new Message();
		m.setOperation("retrieve");
		List<DataEntity> hasPart = new ArrayList<DataEntity>();
		DataObject dObject = new DataObject();
		dObject.setLabel("/databook/home/rods/t1");
		m.setHasPart(hasPart);
		this.scheduler.submit(new Job<Object>(null, m, new Scheduler.Continuation<Object>() {

			@Override
			public void call(Object data) {
				System.out
						.println("********************** job **********************");
			}

		}, null));

	}

	private void connect() {
		String os = getOSVersion();
		// make sure that the nodeBuilder uses the classloader for the
		// elasticsearch.jar file
		// this is not always the same as the classloader for this class in an
		// osgi bundle
		Settings settings = ImmutableSettings.settingsBuilder()
				.classLoader(Settings.class.getClassLoader())
				.put("cluster.name", "databookIndexer").build();

		if (os.contains("CentOS") || AS_CLIENT) {
			client = new TransportClient(settings)
					.addTransportAddress(new InetSocketTransportAddress(
							"localhost", 9300));
		} else {

			node = nodeBuilder().settings(settings).client(true).node();
			client = node.client();
		}
	}

	@Override
	public void shutdown() {
		super.shutdown();
		if (node != null) {
			node.close();
		} else {
			client.close();
		}
	}

	private class ESRule implements EntityRule<DataEntity, Object> {
		@Override
		public void create(DataEntity o, Object context) {
			try {
				prePropObjForIndexing(o);
				String s = om.writeValueAsString(o);
				System.out.println("String :" + s);
				IndexResponse resp = client.prepareIndex("databook", "entity")
						.setSource(s).execute().actionGet();
				final String id = resp.getId();
				System.out.println("indexer response " + resp);

				if (o instanceof DataObject) {
					fulltext((DataObject) o, id);
				}
			} catch (Exception e) {
				log.error("error", e);
			}

		}

		@Override
		public void delete(DataEntity o, Object context) {

			prePropObjForIndexing(o);
			DeleteByQueryResponse response = client
					.prepareDeleteByQuery("databook")
					.setQuery(termQuery("uri", o.getUri().toString()))
					.execute().actionGet();
		}

		@Override
		public void modify(DataEntity e0, DataEntity e1, Object context) {

			handleModify(e0, e1);
		}

		@Override
		public void union(DataEntity o0, DataEntity o1, Object context) {
			prePropObjForIndexing(o1);

			String id = getId(o0);

			final StringBuilder script = new StringBuilder("");
			final HashMap<String, Object> updateObject = new HashMap<String, Object>();

			mapProperties(o1, new Continuation<Property>() {

				@Override
				public void call(Property data) {
					Object v = data.Value;
					String field = data.key;

					if (v != null
							&& !(v instanceof java.util.Collection && ((java.util.Collection<?>) v)
									.isEmpty())) {
						updateObject.put(field, formatValue(v));
						if (v instanceof java.util.Collection) {
							script.append("if (ctx._source.containsKey(\""
									+ field + "\")) {ctx._source." + field
									+ " += " + field + " ; } else {"
									+ "ctx._source." + field + " = " + field
									+ " ; }");
						} else {
							script.append("ctx._source." + field + " = "
									+ field + " ; ");
						}
					}

				}

			});
			UpdateResponse r = client.prepareUpdate("databook", "entity", id)
					.setScript(script.toString()).setScriptParams(updateObject)
					.execute().actionGet();
			

		}
		

		@Override
		public void diff(DataEntity e0, DataEntity e1, Object context) {
			prePropObjForIndexing(e1);
			String id = getId(e0);

			final StringBuilder script = new StringBuilder("");
			final HashMap<String, Object> updateObject = new HashMap<String, Object>();

			mapProperties(e1, new Continuation<Property>() {

				@Override
				public void call(Property data) {
					Object v = data.Value;
					String field = data.key;

					if (v != null
							&& !(v instanceof java.util.Collection && ((java.util.Collection<?>) v)
									.isEmpty())) {
						updateObject.put(field, formatValue(v));
						if (v instanceof java.util.Collection) {
							script.append("ctx._source." + field
									+ ".removeAll(" + field + ") ; ");
						} else {
							script.append("ctx._source." + field + ".delete(\""
									+ field + "\") ; ");
						}
					}

				}

			});
			UpdateResponse r = client.prepareUpdate("databook", "entity", id)
					.setScript(script.toString()).setScriptParams(updateObject)
					.execute().actionGet();

		}
	}

	public static class Property<T> {
		public String key;
		public T Value;
	}

	public static void mapProperties(DataEntity o1, Continuation<Property> cont) {
		try {
			BeanInfo bi = java.beans.Introspector.getBeanInfo(o1.getClass());
			PropertyDescriptor[] pds = bi.getPropertyDescriptors();
			for (PropertyDescriptor pd : pds) {
				String name = pd.getName();
				// exclude fields
				if (name.equals("type") || name.equals("class")
						|| name.equals("additionalProperties")
						|| name.equals("uri")) {
					continue;
				}
				Method mth = pd.getReadMethod();
				Property<Object> p = new Property<Object>();
				p.key = name;
				p.Value = mth.invoke(o1);
				cont.call(p);
			}
			for (Map.Entry<String, Object> key : o1.getAdditionalProperties()
					.entrySet()) {
				Property<Object> p = new Property<Object>();
				p.key = key.getKey();
				p.Value = key.getValue();
				cont.call(p);
			}
		} catch (Exception e) {
			log.error("error", e);
		}

	}

	private void fulltext(DataObject o, final String id) {
		setLabel(o);

		if (o.getLabel().endsWith(".txt")) {
			System.out.println("full text");
			Message msg = new Message();
			msg.setOperation("retrieve");
			ArrayList<DataEntity> list = new ArrayList<DataEntity>();
			list.add(o);
			msg.setHasPart(list);
			scheduler.submit(new Job<Reader>(this, msg,
					new Continuation<Reader>() {

						@Override
						public void call(Reader data) {
							try {
								Reader is = data;
								String s = IOUtils.toString(is);
								is.close();
								final HashMap<String, Object> updateObject = new HashMap<String, Object>();
								updateObject.put("fulltext", s);
								System.out.println("fulltext: " + s);
								String script = "ctx._source.fulltext = fulltext ; ";

								UpdateResponse r = client
										.prepareUpdate("databook", "entity", id)
										.setScript(script)
										.setScriptParams(updateObject)
										.execute().actionGet();
							} catch (Exception e) {
								log.error("error", e);
							}
						}
					}, new Continuation<Throwable>() {

						@Override
						public void call(Throwable data) {
							log.error("error", data);
						}
					}));
		}

	}

	EntityRule<DataEntity, Object> esRule = new ESRule();
	{
		ruleRegistry.registerRule(DataEntity.class, esRule);
	}
	ObjectMapper om = new ObjectMapper();

	@Override
	public void messages(Messages ms) {
		try {
			// System.out.println("messages received " + ms);
			// om.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
			String s = om.writeValueAsString(ms);
			IndexResponse resp = client.prepareIndex("databook", "events")
					.setSource(s).execute().actionGet();
			System.out.println("indexer response " + resp);

		} catch (Exception e) {
			log.error("error", e);
		}
		super.messages(ms);
	}

	private void handleModify(final DataEntity o0, DataEntity o1) {
		// process web update
		List<StorageLocation> sl = o0.getStorageLocation();
		if (sl != null && sl.contains(StorageLocation.IRODS)) {
			// currently only one avu set
			final AVU oldAVU = o0.getMetadata().get(0);
			final AVU newAVU = o1.getMetadata().get(0);

			System.out.println("set avu");
			Message msg = new Message();
			msg.setOperation("accessObject");
			ArrayList<DataEntity> list = new ArrayList<DataEntity>();
			list.add(o0);
			msg.setHasPart(list);
			scheduler.submit(new Job<DataObjectAO>(this, msg,
					new Continuation<DataObjectAO>() {

						@Override
						public void call(DataObjectAO data) {
							try {
								AvuData avu = new AvuData(
										newAVU.getAttribute(), newAVU
												.getValue(), newAVU.getUnit());
								AVUQueryElement e0 = AVUQueryElement
										.instanceForValueQuery(
												AVUQueryPart.ATTRIBUTE,
												AVUQueryOperatorEnum.EQUAL,
												avu.getAttribute());
								AVUQueryElement e1 = AVUQueryElement
										.instanceForValueQuery(
												AVUQueryPart.UNITS,
												AVUQueryOperatorEnum.EQUAL,
												avu.getUnit());
								List<AVUQueryElement> arg0 = new ArrayList<AVUQueryElement>();
								arg0.add(e0);
								arg0.add(e1);
								String path = o0.getLabel();
								// need to lock object
								List<MetaDataAndDomainData> res = data
										.findMetadataValuesForDataObjectUsingAVUQuery(
												arg0, path);
								if (res.size() == 0) {
									data.addAVUMetadata(o0.getLabel(), avu);
								} else {
									data.modifyAvuValueBasedOnGivenAttributeAndUnit(
											path, avu);
								}
							} catch (Exception e) {
								log.error("error", e);
							}
						}
					}, new Continuation<Throwable>() {

						@Override
						public void call(Throwable data) {
							log.error("error", data);
						}
					}));

		} else {
			prePropObjForIndexing(o1);
			System.out.println("modify : " + o0.getUri());

			String id = getId(o0);

			System.out.println("modify id : " + id);

			final StringBuilder script = new StringBuilder("");
			final HashMap<String, Object> updateObject = new HashMap<String, Object>();

			mapProperties(o1, new Continuation<Property>() {

				@Override
				public void call(Property data) {
					Object v = data.Value;
					String field = data.key;

					if (v != null
							&& !(v instanceof java.util.Collection && ((java.util.Collection<?>) v)
									.isEmpty())) {
						updateObject.put(field, formatValue(v));
						script.append("ctx._source." + field + " = " + field
								+ " ; ");
					}

				}

			});
			System.out.println("script : " + script);
			UpdateResponse r = client.prepareUpdate("databook", "entity", id)
					.setScript(script.toString()).setScriptParams(updateObject)
					.execute().actionGet();
			System.out.println("response : " + r);

			if (o1 instanceof DataObject
					&& ((DataObject) o1).getSubmitted() != null) {
				fulltext((DataObject) o0, id);
			}

		}
	}

	private void setLabel(DataObject o) {
		if (o.getLabel() == null) {
			SearchResponse response = client.prepareSearch("databook")
					.setQuery(termQuery("uri", o.getUri().toString()))
					.execute().actionGet();

			o.setLabel((String) response.getHits().getAt(0).sourceAsMap()
					.get("label"));
		}
	}

	private String getId(DataEntity o) {
		SearchResponse response = client.prepareSearch("databook")
				.setQuery(termQuery("uri", o.getUri().toString())).execute()
				.actionGet();

		return response.getHits().getAt(0).getId();

	}
	
	private String getCollUri(DataEntity o) {
		long total = -1;
		SearchHits hits = null;
		int count = 0;
		int max = 10;
		do {
			if(total!=-1) {
				try{
					Thread.sleep(100);
				
				} catch(Exception e) {
					e.printStackTrace();
				}
			}
			
			System.out.println("getCollUri: uri = " + o.getUri());
			SearchResponse response = client.prepareSearch("databook")
					.setQuery(termQuery("uri", o.getUri().toString())).execute()
					.actionGet();
	
			System.out.println("getCollUri: response = " + response);
			
			hits = response.getHits();
			total = hits.getTotalHits();
			count ++;
		} while(total == 0 && count <= max); // try 10 times, if can't find it give up
		return count == max ? null : (String)((List<String>)hits.getAt(0).sourceAsMap().get("partOfUri")).get(0);
		
	}



	private void prePropObjForIndexing(DataEntity o) {
		List<DataEntity> partOf;
		User owner;
		List<String> cUri = new ArrayList<String>();
		System.out.println("type = " + o.getClass().getName());
		if(o instanceof Access) {
			try {
			List<DataEntityLink> dels = ((Access) o).getLinkingDataEntity();
			DataEntityLink del = new DataEntityLink();
			Collection c = new Collection();
			String curi = getCollUri(dels.get(0).getDataEntity());
			if(curi!=null) {
				c.setUri(new java.net.URI(curi));
				del.setDataEntity(c);
				dels.add(del);
			}
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
		if ((partOf = o.getPartOf()) != null) {

			o.setPartOf(null);
			for (DataEntity coll : partOf) {
				cUri.add(coll.getUri().toString());
			}
			o.setAdditionalProperty("partOfUri", cUri);
		}
		List<AVU> metadata;
		List<Map<String, Object>> metadataObject = new ArrayList<Map<String, Object>>();
		if ((metadata = o.getMetadata()) != null) {
			o.setMetadata(null);
			for (AVU coll : metadata) {
				Map<String, Object> map = new HashMap<String, Object>();
				map.put("attribute", coll.getAttribute());
				map.put("value", coll.getValue());
				map.put("unit", coll.getUnit());
				metadataObject.add(map);
			}
			o.setAdditionalProperty("metadataObject", metadataObject);
		}
		if ((owner = o.getOwner()) != null) {

			o.setOwner(null);
			o.setAdditionalProperty("ownerUri", owner.getUri());
		}
		o.setType(o.getClass().getName());
	}


	private Object formatValue(Object v) {
		if (v instanceof java.util.Date) {
			return ((java.util.Date) v).getTime();
		} else {
			return v;
		}
	}

}
