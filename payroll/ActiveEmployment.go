package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"

	errortools "github.com/leapforce-libraries/go_errortools"
	payroll "github.com/leapforce-libraries/go_exactonline_new/payroll"
	go_bigquery "github.com/leapforce-libraries/go_google/bigquery"
	types "github.com/leapforce-libraries/go_types"
)

type ActiveEmployment struct {
	OrganisationID_          int64
	SoftwareClientLicenceID_ int64
	ID                       string
	AverageDaysPerWeek       float64
	AverageHoursPerWeek      float64
	Contract                 string
	ContractDocument         string
	ContractEndDate          bigquery.NullTimestamp
	ContractProbationEndDate bigquery.NullTimestamp
	ContractProbationPeriod  int64
	ContractStartDate        bigquery.NullTimestamp
	ContractType             int64
	ContractTypeDescription  string
	Created                  bigquery.NullTimestamp
	Creator                  string
	CreatorFullName          string
	Department               string
	DepartmentCode           string
	DepartmentDescription    string
	Division                 int64
	Employee                 string
	EmployeeFullName         string
	EmployeeHID              int64
	EmploymentOrganization   string
	EndDate                  bigquery.NullTimestamp
	HID                      int64
	HourlyWage               float64
	InternalRate             float64
	Jobtitle                 string
	JobtitleDescription      string
	Modified                 bigquery.NullTimestamp
	Modifier                 string
	ModifierFullName         string
	ReasonEnd                int64
	ReasonEndDescription     string
	ReasonEndFlex            int64
	ReasonEndFlexDescription string
	Salary                   string
	Schedule                 string
	ScheduleAverageHours     float64
	ScheduleCode             string
	ScheduleDays             float64
	ScheduleDescription      string
	ScheduleHours            float64
	StartDate                bigquery.NullTimestamp
	StartDateOrganization    bigquery.NullTimestamp
}

func getActiveEmployment(c *payroll.ActiveEmployment, organisationID int64, softwareClientLicenceID int64) ActiveEmployment {
	return ActiveEmployment{
		organisationID,
		softwareClientLicenceID,
		c.ID.String(),
		c.AverageDaysPerWeek,
		c.AverageHoursPerWeek,
		c.Contract.String(),
		c.ContractDocument.String(),
		go_bigquery.DateToNullTimestamp(c.ContractEndDate),
		go_bigquery.DateToNullTimestamp(c.ContractProbationEndDate),
		c.ContractProbationPeriod,
		go_bigquery.DateToNullTimestamp(c.ContractStartDate),
		c.ContractType,
		c.ContractTypeDescription,
		go_bigquery.DateToNullTimestamp(c.Created),
		c.Creator.String(),
		c.CreatorFullName,
		c.Department.String(),
		c.DepartmentCode,
		c.DepartmentDescription,
		c.Division,
		c.Employee.String(),
		c.EmployeeFullName,
		c.EmployeeHID,
		c.EmploymentOrganization.String(),
		go_bigquery.DateToNullTimestamp(c.EndDate),
		c.HID,
		c.HourlyWage,
		c.InternalRate,
		c.Jobtitle.String(),
		c.JobtitleDescription,
		go_bigquery.DateToNullTimestamp(c.Modified),
		c.Modifier.String(),
		c.ModifierFullName,
		c.ReasonEnd,
		c.ReasonEndDescription,
		c.ReasonEndFlex,
		c.ReasonEndFlexDescription,
		c.Salary.String(),
		c.Schedule.String(),
		c.ScheduleAverageHours,
		c.ScheduleCode,
		c.ScheduleDays,
		c.ScheduleDescription,
		c.ScheduleHours,
		go_bigquery.DateToNullTimestamp(c.StartDate),
		go_bigquery.DateToNullTimestamp(c.StartDateOrganization),
	}
}

func (service *Service) WriteActiveEmployments(bucketHandle *storage.BucketHandle, organisationID int64, softwareClientLicenceID int64, lastModified *time.Time) ([]*storage.ObjectHandle, int, interface{}, *errortools.Error) {
	if bucketHandle == nil {
		return nil, 0, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.PayrollService().NewGetActiveEmploymentsCall(lastModified)

	rowCount := 0
	batchRowCount := 0
	batchSize := 10000

	for true {
		activeEmployments, e := call.Do()
		if e != nil {
			return nil, 0, nil, e
		}

		if activeEmployments == nil {
			break
		}

		if batchRowCount == 0 {
			guid := types.NewGUID()
			objectHandle := bucketHandle.Object((&guid).String())
			objectHandles = append(objectHandles, objectHandle)

			w = objectHandle.NewWriter(context.Background())
		}

		for _, tl := range *activeEmployments {
			batchRowCount++

			b, err := json.Marshal(getActiveEmployment(&tl, organisationID, softwareClientLicenceID))
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}

			// Write data
			_, err = w.Write(b)
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}

			// Write NewLine
			_, err = fmt.Fprintf(w, "\n")
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}
		}

		if batchRowCount > batchSize {
			// Close and flush data
			err := w.Close()
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}
			w = nil

			fmt.Printf("#ActiveEmployments flushed: %v\n", batchRowCount)

			rowCount += batchRowCount
			batchRowCount = 0
		}
	}

	if w != nil {
		// Close and flush data
		err := w.Close()
		if err != nil {
			return nil, 0, nil, errortools.ErrorMessage(err)
		}

		rowCount += batchRowCount
	}

	fmt.Printf("#ActiveEmployments: %v\n", rowCount)

	return objectHandles, rowCount, ActiveEmployment{}, nil
}
