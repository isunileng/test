package com.mmi.bpm.dao;



import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import com.mmi.bpm.bean.ActivityBean;
import com.mmi.bpm.bean.ActivityMappingBean;
import com.mmi.bpm.bean.ProcessBean;
import com.mmi.bpm.bean.ProcessTableBean;
import com.mmi.bpm.bean.ProcessTableInfo;
import com.mmi.bpm.bean.RuleBean;
import com.mmi.bpm.bean.RuleInfo;

@Component
public class BPMDao {

	static Logger logger = Logger.getLogger(BPMDao.class.getName());

	@Autowired
    private DataSource dataSource;

    public List<ProcessBean> getProcessList() {
    	
    	JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    	List<ProcessBean> ls = jdbcTemplate.query("select process_id, process_name, process_identifier from mst_process",
    		new Object[]{}, new RowMapper<ProcessBean>() {
            @Override
            public ProcessBean mapRow(ResultSet rs, int rowNum) throws SQLException {
            	ProcessBean processBean = new ProcessBean();
            	processBean.setProcessId(rs.getInt("process_id"));
            	processBean.setProcessName(rs.getString("process_name"));
            	processBean.setProcessIdentifier(rs.getString("process_identifier"));
                return processBean;
            }
        });
    	return ls;
    }
    
    public List<ActivityBean> getActivityList(int process_id) {
    	
    	JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    	List<ActivityBean> ls = jdbcTemplate.query("select process_id, activity_id, activity_name, activity_type from mst_activity where process_id = ?",
    		new Object[]{process_id}, new RowMapper<ActivityBean>() {
            @Override
            public ActivityBean mapRow(ResultSet rs, int rowNum) throws SQLException {
            	ActivityBean activityBean = new ActivityBean();
            	activityBean.setProcessId(rs.getInt("process_id"));
            	activityBean.setActivityId(rs.getInt("process_id"));
            	activityBean.setActivityName(rs.getString("activity_name"));
            	activityBean.setActivityType(rs.getString("activity_type"));
                return activityBean;
            }
        });
    	return ls;
    }
    
    public List<ActivityMappingBean> getActivityMapping(int process_id) {
    	
    	JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    	List<ActivityMappingBean> ls = jdbcTemplate.query("select process_id, activity_id, activity_instream, activity_outstream from mst_activity_mapping where process_id = ?",
    		new Object[]{process_id}, new RowMapper<ActivityMappingBean>() {
            @Override
            public ActivityMappingBean mapRow(ResultSet rs, int rowNum) throws SQLException {
            	ActivityMappingBean activityMappingBean = new ActivityMappingBean();
            	activityMappingBean.setProcessId(rs.getInt("process_id"));
            	activityMappingBean.setActivityId(rs.getInt("process_id"));
            	activityMappingBean.setActivityInstream(rs.getString("activity_instream"));
            	activityMappingBean.setActivityOutstream(rs.getString("activity_outstream"));
                return activityMappingBean;
            }
        });
    	return ls;
    }
    
    public List<RuleBean> getProcessRule(int process_id) {
    	
    	JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    	List<RuleBean> ls = jdbcTemplate.query("SELECT process_id, rule_id FROM mst_process_rules WHERE process_id=?",
    		new Object[]{process_id}, new RowMapper<RuleBean>() {
            @Override
            public RuleBean mapRow(ResultSet rs, int rowNum) throws SQLException {
            	RuleBean ruleBean = new RuleBean();
            	ruleBean.setRuleId(rs.getString("rule_id"));
                return ruleBean;
            }
        });
    	
    	return ls;
    }
    
    public List<RuleInfo> getProcessRuleInfo(String rule_id) {
    	
    	JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    	List<RuleInfo> ls = jdbcTemplate.query("SELECT rule_id,rule_sequence, table_name, operand_lhs, operand_rhs, operator, expression " + 
    			" FROM cmp_process_rules_info WHERE rule_id = ?",
    		new Object[]{rule_id}, new RowMapper<RuleInfo>() {
            @Override
            public RuleInfo mapRow(ResultSet rs, int rowNum) throws SQLException {
            	RuleInfo ruleInfo = new RuleInfo();
            	ruleInfo.setRule_id(rs.getString("rule_id"));
            	ruleInfo.setRule_sequence(rs.getInt("rule_sequence"));
            	ruleInfo.setTable_name(rs.getString("table_name"));
            	ruleInfo.setOperand_lhs(rs.getString("operand_lhs"));
            	ruleInfo.setOperand_rhs(rs.getString("operand_rhs"));
            	ruleInfo.setOperator(rs.getString("operator"));
            	ruleInfo.setExpression(rs.getString("expression"));
                return ruleInfo;
            }
        });
    	
    	return ls;
    }
    
    public List<ProcessTableBean> getProcessTable(int process_id) {
    	
    	JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    	List<ProcessTableBean> ls = jdbcTemplate.query("SELECT process_id, table_name, table_category FROM  mst_process_table_info WHERE process_id=?",
    		new Object[]{process_id}, new RowMapper<ProcessTableBean>() {
            @Override
            public ProcessTableBean mapRow(ResultSet rs, int rowNum) throws SQLException {
            	ProcessTableBean processTableInfo = new ProcessTableBean();
            	processTableInfo.setProcess_id(rs.getInt("process_id"));
            	processTableInfo.setTable_name(rs.getString("table_name"));
            	processTableInfo.setTable_category(rs.getString("table_category"));
            	
                return processTableInfo;
            }
        });
    	
    	return ls;
    }
    
    public List<ProcessTableInfo> getProcessTableInfo(String table_name) {
    	
    	JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    	List<ProcessTableInfo> ls = jdbcTemplate.query("SELECT table_name, column_name, column_type FROM cmp_process_table_info WHERE table_name = ?",
    		new Object[]{table_name}, new RowMapper<ProcessTableInfo>() {
            @Override
            public ProcessTableInfo mapRow(ResultSet rs, int rowNum) throws SQLException {
            	ProcessTableInfo processTableInfo = new ProcessTableInfo();
            	processTableInfo.setTable_name(rs.getString("table_name"));
            	processTableInfo.setColumn_name(rs.getString("column_name"));
            	processTableInfo.setColumn_type(rs.getString("column_type"));
                return processTableInfo;
            }
        });
    	
    	return ls;
    }
    
}
