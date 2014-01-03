package com.cisco.webex.bigdata.kaifang;

import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.solr.client.solrj.SolrServerException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping("/kf")
public class HBaseQueryKFController {

	@Autowired
	private KFService kaifangService;
	
	@RequestMapping(value="/{cardID}",method=RequestMethod.POST)
	public @ResponseBody ActionResult<KFUserBean> getKFUserByID(@PathVariable String cardID){
		ActionResult<KFUserBean> result=new ActionResult<KFUserBean>();
		
		List<KFUserBean> users=kaifangService.getKFUserInfoByCardID(cardID);
		result.success();
		result.setPayloadList(users);
		return result;
	}
	
	@RequestMapping(value="/get/{username}",method=RequestMethod.POST)
	public @ResponseBody ActionResult<KFUserBean> getKFUserByUserName(@PathVariable String username) throws UnsupportedEncodingException{
		ActionResult<KFUserBean> result=new ActionResult<KFUserBean>();
		
		List<KFUserBean> users=null;
		try {
			users = kaifangService.getKFUserInfoByUserName(username);
		} catch (SolrServerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		result.success();
		result.setPayloadList(users);
		return result;
	}
}
