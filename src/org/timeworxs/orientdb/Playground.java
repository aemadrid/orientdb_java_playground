/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.timeworxs.orientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.io.FileUtils;

/**
 *
 * @author aemadrid
 */
public class Playground {

  static final Logger logr = Logger.getLogger(Playground.class.getName());
  static final String PATH = "/Users/aemadrid/code/aem/odb/client/tmp/databases";
  ODatabaseDocumentTx db = null;
  OSchema schema = null;
  OClass person = null;
  OClass tag = null;

  public static void main(String[] args) {
    Playground pg = new Playground();
  }

  public Playground() {
    init();
    run();
    finish();
  }

  private void init() {
    dline();
    log("INIT starting...");
    line();
    // Setup the db
    try {
      log("Removing [" + PATH + "]...");
      FileUtils.deleteDirectory(new File(PATH));
    } catch (IOException ex) {
      logr.log(Level.SEVERE, null, ex);
    }
    db = new ODatabaseDocumentTx("local:" + PATH + "/playground").create();
    log("DB", db);

    // Schema
    schema = db.getMetadata().getSchema();
    log("Schema", schema);

    line();
    // ============================================== TAGS ==============================================
    log("Tags");
    tag = schema.createClass("Tag");
    tag.createProperty("name", OType.STRING);

    // Fighter
    ODocument fighter = new ODocument(db, "Tag");
    fighter.field("name", "fighter");
    fighter.save();
    log(fighter);

    // Jedi
    ODocument jedi = new ODocument(db, "Tag");
    jedi.field("name", "jedi");
    jedi.save();
    log(jedi);

    // Princess
    ODocument princess = new ODocument(db, "Tag");
    princess.field("name", "princess");
    princess.save();
    log(princess);

    // Cool
    ODocument cool = new ODocument(db, "Tag");
    cool.field("name", "cool");
    cool.save();
    log(cool);

    // Hairy
    ODocument hairy = new ODocument(db, "Tag");
    hairy.field("name", "hairy");
    hairy.save();
    log(hairy);

    // Short
    ODocument shorty = new ODocument(db, "Tag");
    shorty.field("name", "short");
    shorty.save();
    log(shorty);

    line();
    // ============================================== PEOPLE ==============================================
    log("People");
    person = schema.createClass("Person");
    person.createProperty("name", OType.STRING);
    person.createProperty("age", OType.INTEGER);
    person.createProperty("tags", OType.EMBEDDEDLIST, tag);
    person.createProperty("stags", OType.EMBEDDEDLIST, OType.STRING);
    log("person", person);

    // Luke
    ODocument luke = new ODocument(db, "Person");
    luke.field("name", "Luke");
    luke.field("age", 36);
    luke.field("tags", c_otags(jedi, cool));
    luke.field("stags", c_stags("jedi", "cool"));
    luke.save();
    log(luke.toString() + " : " + luke.field("tags").toString() + " : " + luke.field("stags").toString());

    // Yoda
    ODocument yoda = new ODocument(db, "Person");
    yoda.field("name", "Yoda");
    yoda.field("age", 1435);
    yoda.field("tags", c_otags(jedi, shorty));
    yoda.field("stags", c_stags("jedi", "shorty"));
    yoda.save();
    log(yoda.toString() + " : " + yoda.field("tags").toString() + " : " + yoda.field("stags").toString());

    // Leia
    ODocument leia = new ODocument(db, "Person");
    leia.field("name", "Leia");
    leia.field("age", 34);
    leia.field("tags", c_otags(cool, princess));
    leia.field("stags", c_stags("cool", "princess"));
    leia.save();
    log(leia.toString() + " : " + leia.field("tags").toString() + " : " + leia.field("stags").toString());

    // Han
    ODocument han = new ODocument(db, "Person");
    han.field("name", "Han");
    han.field("age", 38);
    han.field("tags", c_otags(cool, fighter));
    han.field("stags", c_stags("cool", "fighter"));
    han.save();
    log(han.toString() + " : " + han.field("tags").toString() + " : " + han.field("stags").toString());

    // Chewbaca
    ODocument chewb = new ODocument(db, "Person");
    chewb.field("name", "Chewbaca");
    chewb.field("age", 45);
    chewb.field("tags", c_otags(hairy, fighter));
    chewb.field("stags", c_stags("hairy", "fighter"));
    chewb.save();
    log(chewb.toString() + " : " + chewb.field("tags").toString() + " : " + chewb.field("stags").toString());

    line();
    log("INIT ending...");
  }

  private void run() {
    dline();
    log("RUN starting...");

    line();
    // ---------------------------------------------- Q1 ----------------------------------------------
    log("[1] Get all fighters...");
    String qry1str = "SELECT * FROM person WHERE stags IN ['fighter']";
    OSQLSynchQuery<ODocument> qry1 = new OSQLSynchQuery<ODocument>(qry1str);
    log(qry1);
    List<ODocument> res1 = db.query(qry1);
    log("Returned " + res1.size() + " rows.");
    for (ODocument doc : res1) {
      log(doc);
    }

    line();
    // ---------------------------------------------- Q2 ----------------------------------------------
    log("[2] Get all fighters...");
    String qry2str = "SELECT * FROM person WHERE 'fighter' IN stags";
    OSQLSynchQuery<ODocument> qry2 = new OSQLSynchQuery<ODocument>(qry2str);
    log(qry2);
    List<ODocument> res2 = db.query(qry2);
    log("Returned " + res2.size() + " rows.");
    for (ODocument doc : res2) {
      log(doc);
    }

    line();
    // ---------------------------------------------- Q3 ----------------------------------------------
    log("[3] Get all jedis...");
    String qry3str = "SELECT * FROM person WHERE tags contains (name = 'jedi')";
    OSQLSynchQuery<ODocument> qry3 = new OSQLSynchQuery<ODocument>(qry3str);
    log(qry3);
    List<ODocument> res3 = db.query(qry3);
    log("Returned " + res3.size() + " rows.");
    for (ODocument doc : res3) {
      log(doc);
    }

    line();
    // ---------------------------------------------- Q4 ----------------------------------------------
    log("[4] Get all fighters...");
    String qry4str = "SELECT * FROM person WHERE tags CONTAINS (name = 'fighter')";
    OSQLSynchQuery<ODocument> qry4 = new OSQLSynchQuery<ODocument>(qry4str);
    log(qry4);
    List<ODocument> res4 = db.query(qry4);
    log("Returned " + res4.size() + " rows.");
    for (ODocument doc : res4) {
      log(doc);
    }

    // ---------------------------------------------- Q5 ----------------------------------------------
    line();
    log("[5] Get all cool...");
    String qry5str = "SELECT * FROM person WHERE tags contains (name = 'cool')";
    OSQLSynchQuery<ODocument> qry5 = new OSQLSynchQuery<ODocument>(qry5str);
    log(qry5);
    List<ODocument> res5 = db.query(qry5);
    log("Returned " + res5.size() + " rows.");
    for (ODocument doc : res5) {
      log(doc);
    }

    // ---------------------------------------------- Q6 ----------------------------------------------
    line();
    log("[6] Get all cool fighters...");
    String qry6str = "SELECT * FROM person WHERE ['fighter','cool'] IN stags";
    OSQLSynchQuery<ODocument> qry6 = new OSQLSynchQuery<ODocument>(qry6str);
    log(qry6);
    List<ODocument> res6 = db.query(qry6);
    log("Returned " + res6.size() + " rows.");
    for (ODocument doc : res6) {
      log(doc);
    }

    // ---------------------------------------------- Q7 ----------------------------------------------
    line();
    log("[7] Get all cool fighters...");
    String qry7str = "SELECT * FROM person WHERE ('fighter' IN stags) AND ('cool' IN stags)";
    OSQLSynchQuery<ODocument> qry7 = new OSQLSynchQuery<ODocument>(qry7str);
    log(qry7);
    List<ODocument> res7 = db.query(qry7);
    log("Returned " + res7.size() + " rows.");
    for (ODocument doc : res7) {
      log(doc);
    }

    // ---------------------------------------------- Q8 ----------------------------------------------
    line();
    log("[8] Get all cool fighters...");
    String qry8str = "SELECT * FROM person WHERE (tags contains (name = 'fighter')) AND (tags contains (name = 'cool'))";
    OSQLSynchQuery<ODocument> qry8 = new OSQLSynchQuery<ODocument>(qry8str);
    log(qry8);
    List<ODocument> res8 = db.query(qry8);
    log("Returned " + res8.size() + " rows.");
    for (ODocument doc : res8) {
      log(doc);
    }

    // ---------------------------------------------- Q9 ----------------------------------------------
    line();
    log("[9] Get all cool/fighters...");
    String qry9str = "SELECT * FROM person WHERE (tags contains (name = 'fighter')) OR (tags contains (name = 'cool'))";
    OSQLSynchQuery<ODocument> qry9 = new OSQLSynchQuery<ODocument>(qry9str);
    log(qry9);
    List<ODocument> res9 = db.query(qry9);
    log("Returned " + res9.size() + " rows.");
    for (ODocument doc : res9) {
      log(doc);
    }

    line();
    log("RUN ending...");
  }
  
  private void finish() {
    dline();
    log("FINISH starting...");
    db.close();
    log("FINISH ending...");
  }

  private HashSet c_otags(ODocument i1, ODocument i2) {
    HashSet<ODocument> hs = new HashSet();
    hs.add(i1);
    hs.add(i2);
    return hs;        
  }

  private HashSet c_stags(String i1, String i2) {
    HashSet<String> hs = new HashSet();
    hs.add(i1);
    hs.add(i2);
    return hs;        
  }


  private void log(String msg) {
    System.out.print(msg + "\n");
  }

  private void log(Object obj) {
    System.out.print(obj.toString() + "\n");
  }

  private void log(String msg, Object obj) {
    System.out.print(msg + " : " + obj.toString() + "\n");
  }

  private void line() {
    System.out.print("--------------------------------------------------------------------------------------------------\n");
  }

  private void dline() {
    System.out.print("==================================================================================================\n");
  }
}
