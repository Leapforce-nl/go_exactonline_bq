package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/storage"

	errortools "github.com/leapforce-libraries/go_errortools"
	financial "github.com/leapforce-libraries/go_exactonline_new/financial"
	types "github.com/leapforce-libraries/go_types"
)

type ReportingBalanceByClassification struct {
	OrganisationID_            int64
	SoftwareClientLicenceID_   int64
	SoftwareClientLicenseGuid_ string
	Created_                   time.Time
	Modified_                  time.Time
	ID                         string
	Amount                     float64
	AmountCredit               float64
	AmountDebit                float64
	BalanceType                string
	ClassificationCode         string
	ClassificationDescription  string
	CostCenterCode             string
	CostCenterDescription      string
	CostUnitCode               string
	CostUnitDescription        string
	Count                      int32
	Division                   int32
	GLAccount                  string
	GLAccountCode              string
	GLAccountDescription       string
	GLScheme                   string
	ReportingPeriod            int32
	ReportingYear              int32
	Status                     int32
	Type                       int32
}

func getReportingBalanceByClassification(c *financial.ReportingBalanceByClassification, organisationID int64, softwareClientLicenceID int64, softwareClientLicenseGuid string) ReportingBalanceByClassification {
	t := time.Now()

	return ReportingBalanceByClassification{
		organisationID,
		softwareClientLicenceID,
		softwareClientLicenseGuid,
		t, t,
		c.ID,
		c.Amount,
		c.AmountCredit,
		c.AmountDebit,
		c.BalanceType,
		c.ClassificationCode,
		c.ClassificationDescription,
		c.CostCenterCode,
		c.CostCenterDescription,
		c.CostUnitCode,
		c.CostUnitDescription,
		c.Count,
		c.Division,
		c.GLAccount.String(),
		c.GLAccountCode,
		c.GLAccountDescription,
		c.GLScheme.String(),
		c.ReportingPeriod,
		c.ReportingYear,
		c.Status,
		c.Type,
	}
}

func (service *Service) WriteReportingBalanceByClassifications(bucketHandle *storage.BucketHandle, organisationID int64, softwareClientLicenceID int64, softwareClientLicenseGuid string, _ *time.Time) ([]*storage.ObjectHandle, int, interface{}, *errortools.Error) {
	if bucketHandle == nil {
		return nil, 0, nil, nil
	}

	schemes, e := service.exactOnlineService.FinancialService.NewGetGLSchemesCall(nil).Do()
	if e != nil {
		return nil, 0, nil, e
	}

	if schemes == nil {
		return nil, 0, nil, errortools.ErrorMessage("NewGetGLSchemesCall.Do() returned nil")
	}

	objectHandles := []*storage.ObjectHandle{}
	rowCount := 0

	for _, scheme := range *schemes {

		currentYear := time.Now().Year()
		year := currentYear //start with current year

		// previous years
		for {

			_objectHandles, _rowCount, e := service.writeReportingBalanceByClassifications(bucketHandle, &scheme, year, organisationID, softwareClientLicenceID, softwareClientLicenseGuid)
			if e != nil {
				return nil, 0, nil, e
			}
			if _rowCount == 0 {
				if year != currentYear { //always do previous year
					break
				}
			}

			objectHandles = append(objectHandles, _objectHandles...)
			rowCount += _rowCount

			year-- //continue with previous year
		}

		year = currentYear + 1 //start with next year

		// next years
		for {
			_objectHandles, _rowCount, e := service.writeReportingBalanceByClassifications(bucketHandle, &scheme, year, organisationID, softwareClientLicenceID, softwareClientLicenseGuid)
			if e != nil {
				return nil, 0, nil, e
			}

			if _rowCount == 0 {
				if year != currentYear { //always do previous year
					break
				}
			}

			objectHandles = append(objectHandles, _objectHandles...)
			rowCount += _rowCount

			year++ //continue with next year
		}
	}

	return objectHandles, rowCount, ReportingBalanceByClassification{}, nil
}

func (service *Service) writeReportingBalanceByClassifications(bucketHandle *storage.BucketHandle, scheme *financial.GLScheme, year int, organisationID int64, softwareClientLicenceID int64, softwareClientLicenseGuid string) ([]*storage.ObjectHandle, int, *errortools.Error) {
	if bucketHandle == nil {
		return nil, 0, nil
	}

	objectHandles := []*storage.ObjectHandle{}

	var w *storage.Writer

	call := service.FinancialService().NewGetReportingBalanceByClassificationsCall(scheme, year)

	rowCount := 0
	batchRowCount := 0
	batchSize := 10000

	for {
		reportingBalanceByClassifications, e := call.Do()
		if e != nil {
			return nil, 0, e
		}

		if reportingBalanceByClassifications == nil {
			break
		}

		if batchRowCount == 0 {
			guid := types.NewGuid()
			objectHandle := bucketHandle.Object((&guid).String())
			objectHandles = append(objectHandles, objectHandle)

			w = objectHandle.NewWriter(context.Background())
		}

		for _, tl := range *reportingBalanceByClassifications {
			batchRowCount++

			b, err := json.Marshal(getReportingBalanceByClassification(&tl, organisationID, softwareClientLicenceID, softwareClientLicenseGuid))
			if err != nil {
				return nil, 0, errortools.ErrorMessage(err)
			}

			// Write data
			_, err = w.Write(b)
			if err != nil {
				return nil, 0, errortools.ErrorMessage(err)
			}

			// Write NewLine
			_, err = fmt.Fprintf(w, "\n")
			if err != nil {
				return nil, 0, errortools.ErrorMessage(err)
			}
		}

		if batchRowCount > batchSize {
			// Close and flush data
			err := w.Close()
			if err != nil {
				return nil, 0, errortools.ErrorMessage(err)
			}
			w = nil

			fmt.Printf("#ReportingBalanceByClassifications flushed: %v\n", batchRowCount)

			rowCount += batchRowCount
			batchRowCount = 0
		}
	}

	if w != nil {
		// Close and flush data
		err := w.Close()
		if err != nil {
			return nil, 0, errortools.ErrorMessage(err)
		}

		rowCount += batchRowCount
	}

	fmt.Printf("#ReportingBalanceByClassifications, scheme %s, reportingYear %v: %v\n", scheme.ID.String(), year, rowCount)

	return objectHandles, rowCount, nil
}
