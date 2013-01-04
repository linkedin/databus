package com.linkedin.databus.core;

import static org.testng.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.linkedin.databus.core.data_model.PhysicalPartition;

public class TestCheckpointMult
{
  CheckpointMult _cpMult;
  int pSrcId1 = 1, pSrcId2 = 2;
  Map<Integer, List<Integer>> pSrcIds = new HashMap<Integer, List<Integer>>();
  Integer WIN_OFFSET1 = 100;
  Integer WIN_SCN1 = 1000;
  Integer WIN_OFFSET2 = 200;
  Integer WIN_SCN2 = 2000;

  @BeforeTest
  void setup() {
     // init test data
    ArrayList<Integer> al = new ArrayList<Integer>();
    al.add(WIN_OFFSET1);
    al.add(WIN_SCN1);
    pSrcIds.put(pSrcId1, al);
    al = new ArrayList<Integer>();
    al.add(WIN_OFFSET2);
    al.add(WIN_SCN2);
    pSrcIds.put(pSrcId2, al);

    _cpMult = new CheckpointMult();

    for(int id: pSrcIds.keySet()) {
      Checkpoint cp = new Checkpoint();
      cp.setWindowOffset(pSrcIds.get(id).get(0));
      cp.setWindowScn((long)pSrcIds.get(id).get(1));
      PhysicalPartition pPart = new PhysicalPartition(id, "name");
      _cpMult.addCheckpoint(pPart, cp);
    }
  }

  @Test
  void testCheckpoint() throws JsonGenerationException, JsonMappingException, IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Checkpoint cp = new Checkpoint();
    cp.setWindowOffset(10);
    cp.setWindowScn(100L);
    assertEquals((int)cp.getWindowOffset(), 10, "window offset mismatch");
    assertEquals(cp.getWindowScn(), 100L, "window scn mismatch");
    cp.serialize(baos);
    //System.out.println("TC: in: " + baos.toString());
    Checkpoint cp1 = new Checkpoint(baos.toString());
    assertEquals((int)cp1.getWindowOffset(), 10, "after deser window offset mismatch");
    assertEquals(cp1.getWindowScn(), 100L, "after deser window scn mismatch");
  }

  public void validateCheckpoints() {
    for(int id : pSrcIds.keySet()) {
      PhysicalPartition pPart = new PhysicalPartition(id, "name");
      Checkpoint cp = _cpMult.getCheckpoint(pPart);
      List<Integer> l = pSrcIds.get(id);
      assertEquals(cp.getWindowOffset(), l.get(0),
                 "window offset in Checkpoint1 doesn't match");

      assertEquals(cp.getWindowScn(), (long)l.get(1),
                 "snapshot offset in Checkpoint1 doesn't match");
    }
  }
  @Test
  public void singleCpConstruct() throws JsonGenerationException, JsonMappingException, IOException {
    // add one more
    Checkpoint cpN = new Checkpoint();
    cpN.setFlexible();
    cpN.setWindowOffset(150);
    cpN.setWindowScn(1500L);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    cpN.serialize(baos);
    //System.out.println("SERDE before:" + _cpMult.getCheckpoint(1));

    String s = baos.toString();
    //System.out.println("singleCpConstruct: ser version of cpMult:" + s);

    // de-ser it
    //CheckpointMult anotherCpMult = new CheckpointMult();
    Checkpoint cp = new Checkpoint(s);
    //PhysicalPartition pPart = new PhysicalPartition(5, "name");
    //anotherCpMult.addCheckpoint(pPart, cp);


    assertEquals(cp.getWindowScn(), cpN.getWindowScn(), "scn offset in new cpMult1 doesn't match");
    assertEquals(cp.getWindowOffset(), cpN.getWindowOffset(), "window offset in int the new cpMult1 doesn't match");
  }

  @Test
  public void serdeCheckpointMult() throws JsonGenerationException, JsonMappingException, IOException {

    validateCheckpoints();
    assertEquals(_cpMult.getNumCheckponts(), 2, " wrong number of enteries in cpMult");


    // serialize checkpoints
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    _cpMult.serialize(baos);
    //System.out.println("SERDE before:" + _cpMult.getCheckpoint(1));

    String s = baos.toString();
    //System.out.println("ser version of cpMult:" + s);

    // de-ser it
    CheckpointMult anotherCpMult = new CheckpointMult(s);
    //System.out.println("SERDE after:" + anotherCpMult.getCheckpoint(1));

    assertEquals(anotherCpMult.getNumCheckponts(), 2,
                 " wrong number of enteries in new cpMult");

    // verify
    for(Integer i : pSrcIds.keySet()) {
      PhysicalPartition pPart = new PhysicalPartition(i, "name");
      Checkpoint cp = _cpMult.getCheckpoint(pPart);
      Checkpoint aCp = anotherCpMult.getCheckpoint(pPart);

      //System.out.println("id = " + i + ";scn=" + cp.getWindowScn() + ";awscn=" + aCp.getWindowScn());
      assertEquals(cp.getWindowScn(), aCp.getWindowScn(),
                   "scn offset in new cpMult doesn't match");
      assertEquals(cp.getWindowOffset(), aCp.getWindowOffset(),
                   "window offset in int the new cpMult doesn't match");
    }
  }
}
