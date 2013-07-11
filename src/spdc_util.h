#define UNKNOWN_ERR				-1
#define SETTINGS_NULL_ERR 		 0
#define INVALID_SETTINGS_ERR 	 1

void SPDC_Abort(int err_code);
void SPDC_Aborts(int err_code, char* err_msg);
int validate_settings(SPDS_Settings* set);