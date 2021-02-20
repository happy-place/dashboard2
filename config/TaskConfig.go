package config

var (
	tasks = []string{
		"dws_file_7d_statistic_by_global_daily", // 最近7天，新建文件统计
		"dws_file_7d_statistic_by_department_daily",
		"dws_file_7d_statistic_by_member_daily",
		"dws_collaboration_7d_statistic_by_global_daily", // 最近7天，协作统计
		"dws_collaboration_7d_statistic_by_department_daily",
		"dws_collaboration_7d_statistic_by_member_daily",
		"dws_collaboration_30d_statistic_by_member_daily", // 最近30天，协作统计
		"dws_file_7d_product_statistic_by_global_daily",   // 最近7天，分产品新建文件统计
		"dws_file_7d_product_statistic_by_department_daily",
		"dws_file_7d_product_statistic_by_member_daily",
		"dws_enterprise_7d_user_statistic_by_global_daily",  // 历史以来企业成员激活席位、活跃统计
		"dws_enterprise_td_usage_statistic_by_global_daily", // 历史以来企业成员分产品新建、协作统计
		"dws_file_1d_hot_statistic_by_global_daily",         // 最近24h热门文件
		"dws_file_7d_hot_statistic_by_global_daily",
		"dws_space_1d_updated_statistic_by_global_daily", // 最新更新空间
		"dws_usage_1d_download_by_member_daily",
		"dws_creation_1d_download_by_global_daily", // 依赖dws_enterprise_7d_user_statistic_by_global_daily,dws_enterprise_td_usage_statistic_by_global_daily,
	}
)

func GetTasks() []string {
	return tasks
}
